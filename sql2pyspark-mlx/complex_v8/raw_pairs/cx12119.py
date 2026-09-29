from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_distinct_holder_ids = Window.partitionBy("distinct_holder_ids")
w_all_by_avg_policy_product_co = Window.orderBy(F.col("avg_policy_product_count").desc_nulls_first(), "end_date_year", "status").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_max_premium_running = Window.orderBy("max_premium", "end_date_year", "status").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_running_max_premium_f = Window.orderBy(F.col("running_max_premium").desc(), "end_date_year", "status").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_distinct_holder_ids_p = Window.orderBy(F.col("distinct_holder_ids").desc_nulls_first(), "end_date_year", "status").rowsBetween(-3, -1)
w_status_end_date_year = Window.partitionBy("status", "end_date_year")
w_all_by_distinct_holder_ids = Window.orderBy(F.col("distinct_holder_ids").desc_nulls_first(), "end_date_year", "status")

prepared = (
    df_policies
    .filter((F.col("status").isin("ACTIVE", "CANCELLED", "LAPSED")) & (F.col("holder_id") >= 3))
    .select(
        "id",
        "holder_id",
        "premium",
        "end_date",
        "status",
        F.concat(F.col("product"), F.lit("#"), F.col("id").cast("string")).alias("product_ref"),
        F.date_format(F.col("start_date"), "yyyy-MM-dd").alias("start_date_label"),
        F.substring(F.trim(F.col("status")), 1, 2).alias("status_prefix"),
    )
)

policy_stats = (
    df_policies
    .groupBy("holder_id")
    .agg(
        F.round(F.avg("premium"), 2).alias("avg_policy_premium"),
        F.min("end_date").alias("first_policy_end_date"),
        F.count(F.when(F.col("product").isin("HEALTH", "TRAVEL"), 1)).alias("policy_product_count"),
    )
)

summarized_rows = (
    prepared.alias("s")
    .join(policy_stats.alias("p"), F.col("p.holder_id") == F.col("s.holder_id"), "left")
    .select(
        "s.*",
        "p.avg_policy_premium",
        "p.first_policy_end_date",
        F.coalesce(F.col("p.policy_product_count"), F.lit(0)).alias("policy_product_count"),
    )
)

summary = (
    summarized_rows
    .filter(F.col("product_ref").isNotNull())
    .groupBy(F.year("end_date").alias("end_date_year"), "status")
    .agg(
        F.countDistinct("holder_id").alias("distinct_holder_ids"),
        F.round(F.avg("policy_product_count"), 2).alias("avg_policy_product_count"),
        F.max("premium").alias("max_premium"),
    )
)

with_windows = (
    summary
    .select(
        "*",
        F.min("max_premium").over(w_distinct_holder_ids).alias("distinct_holder_ids_min_max_premium"),
        F.sum("max_premium").over(w_all_by_avg_policy_product_co).alias("running_max_premium"),
        F.min("end_date_year").over(w_all_by_max_premium_running).alias("min_end_date_year_so_far"),
    )
)

with_trends = (
    with_windows
    .select(
        "end_date_year",
        "status",
        "distinct_holder_ids",
        "avg_policy_product_count",
        "max_premium",
        "distinct_holder_ids_min_max_premium",
        "running_max_premium",
        "min_end_date_year_so_far",
        F.nth_value("max_premium", 2).over(w_all_by_running_max_premium_f).alias("second_max_premium"),
        F.sum("running_max_premium").over(w_all_by_distinct_holder_ids_p).alias("running_max_premium_sum_3_prior"),
        F.round(F.col("max_premium") / F.nullif(F.sum("max_premium").over(w_status_end_date_year), F.lit(0)), 4).alias("max_premium_share_of_status_end_date_year"),
        (F.col("distinct_holder_ids") - F.lag("distinct_holder_ids").over(w_all_by_distinct_holder_ids)).alias("distinct_holder_ids_change"),
        F.count(F.lit(1)).over(w_status_end_date_year).alias("rows_in_status_end_date_year"),
    )
)

result = (
    with_trends
    .select(
        "end_date_year",
        "status",
        "distinct_holder_ids",
        "avg_policy_product_count",
        "max_premium",
        "second_max_premium",
        "running_max_premium_sum_3_prior",
        "max_premium_share_of_status_end_date_year",
        "distinct_holder_ids_change",
        "rows_in_status_end_date_year",
        F.least(F.col("max_premium"), F.col("min_end_date_year_so_far")).alias("least_max_premium_min_end_date_year_so_far"),
        F.upper(F.trim(F.col("status"))).alias("status_clean"),
    )
)
