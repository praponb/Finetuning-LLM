from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_start_date_month_first_polic = Window.partitionBy(F.month("start_date"), F.date_trunc("month", "first_policy_start_date")).orderBy("start_date", "id")
w_status_group_holder_id = Window.partitionBy("status_group", "holder_id")
w_all_by_days_end_date_to_star = Window.orderBy(F.col("days_end_date_to_start_date").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_running_premium_runni = Window.orderBy("running_premium", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_status_group = Window.partitionBy("status_group")
w_seq_no_first_policy_start_da = Window.partitionBy("seq_no", F.year("first_policy_start_date")).orderBy("days_end_date_to_start_date", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_running_premium_full = Window.orderBy("running_premium", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_second_first_policy_start_da = Window.partitionBy(F.to_date("second_first_policy_start_date_month")).orderBy(F.col("prev_days_end_date_to_start_date_status_in_cance").desc(), "first_first_policy_start_date_ym", "second_first_policy_start_date_month")

filtered = (
    df_policies
    .filter((F.col("holder_id").isNotNull()) | (F.col("start_date") < F.lit("2025-05-28").cast("date")))
    .select(
        "id",
        "holder_id",
        "premium",
        "start_date",
        "end_date",
        "status",
        F.when(F.col("status").isin("EXPIRED", "LAPSED"), "group_a").when(F.col("status") == "ACTIVE", "group_b").otherwise("other").alias("status_group"),
        F.datediff(F.col("start_date"), F.col("end_date")).alias("days_end_date_to_start_date"),
        F.upper(F.trim(F.col("product"))).alias("product_clean"),
    )
)

policy_stats = (
    df_policies
    .groupBy("holder_id")
    .agg(F.min("start_date").alias("first_policy_start_date"))
)

with_counts = (
    filtered.alias("s")
    .join(policy_stats.alias("p"), F.col("p.holder_id") == F.col("s.holder_id"), "left")
    .select("s.*", "p.first_policy_start_date")
)

filtered_2 = (
    with_counts.alias("s")
    .join(df_policyholders.alias("p"), (F.col("p.id") == F.col("s.holder_id")) & (F.col("p.holder_name").isNotNull()), "left_semi")
)

ranked = (
    filtered_2
    .select(
        "id",
        "holder_id",
        "end_date",
        "status",
        "status_group",
        "days_end_date_to_start_date",
        "product_clean",
        "first_policy_start_date",
        F.lag("days_end_date_to_start_date").over(w_start_date_month_first_polic).alias("prev_days_end_date_to_start_date"),
        F.count(F.lit(1)).over(w_status_group_holder_id).alias("rows_in_status_group_holder_id"),
        F.row_number().over(w_start_date_month_first_polic).alias("seq_no"),
        F.size(F.collect_set("status").over(w_status_group_holder_id)).alias("distinct_status_in_status_group_holder_id"),
        F.sum("premium").over(w_all_by_days_end_date_to_star).alias("running_premium"),
    )
)

metrics = (
    ranked
    .select(
        "id",
        "holder_id",
        "end_date",
        "status",
        "product_clean",
        "first_policy_start_date",
        "prev_days_end_date_to_start_date",
        "rows_in_status_group_holder_id",
        "seq_no",
        "distinct_status_in_status_group_holder_id",
        "running_premium",
        F.count(F.lit(1)).over(w_all_by_running_premium_runni).alias("running_count"),
        F.sum("running_premium").over(w_status_group).alias("status_group_sum_running_premium"),
        F.first("first_policy_start_date").over(w_seq_no_first_policy_start_da).alias("first_first_policy_start_date"),
        F.nth_value("first_policy_start_date", 2).over(w_all_by_running_premium_full).alias("second_first_policy_start_date"),
    )
)

per_group = (
    metrics
    .groupBy(
        F.date_format("first_first_policy_start_date", "yyyy-MM").alias("first_first_policy_start_date_ym"),
        F.date_trunc("month", "second_first_policy_start_date").alias("second_first_policy_start_date_month"),
    )
    .agg(
        F.sum(F.when(F.col("status").isin("CANCELLED", "LAPSED", "ACTIVE"), F.col("prev_days_end_date_to_start_date")).otherwise(0)).alias("prev_days_end_date_to_start_date_status_in_cance"),
        F.round(F.stddev("status_group_sum_running_premium"), 2).alias("stddev_status_group_sum_running_premium"),
    )
)

sequenced = (
    per_group
    .select(
        "first_first_policy_start_date_ym",
        "second_first_policy_start_date_month",
        "prev_days_end_date_to_start_date_status_in_cance",
        F.round(F.cume_dist().over(w_second_first_policy_start_da), 3).alias("prev_days_end_date_to_start_date_status_in_cance_2"),
    )
)

result = sequenced.select("first_first_policy_start_date_ym")
