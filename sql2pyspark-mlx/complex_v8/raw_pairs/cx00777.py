from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_product = Window.partitionBy("product")
w_status_by_days_end_date_to_s = Window.partitionBy("status").orderBy(F.col("days_end_date_to_start_date").desc_nulls_first(), "id")

scoped_policys = (
    df_policies
    .filter((F.col("premium") < 3817.5) & (F.col("holder_id").isNotNull()))
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "end_date",
        "status",
        F.datediff(F.col("start_date"), F.col("end_date")).alias("days_end_date_to_start_date"),
        F.upper(F.trim(F.col("product"))).alias("product_clean"),
        F.date_format(F.col("start_date"), "yyyy-MM").alias("start_date_label"),
    )
)

with_activity = (
    scoped_policys
    .crossJoin(scoped_policys.agg(F.min("days_end_date_to_start_date").alias("overall_min_days_end_date_to_start_date")))
    .filter(F.col("days_end_date_to_start_date") > F.col("overall_min_days_end_date_to_start_date"))
    .drop("overall_min_days_end_date_to_start_date")
)

holder_id_stats = (
    with_activity
    .groupBy("holder_id")
    .agg(F.avg("premium").alias("avg_premium"))
)

above_average = (
    with_activity.alias("s")
    .join(holder_id_stats.alias("g"), F.col("g.holder_id") == F.col("s.holder_id"), "inner")
    .filter(F.col("s.premium") > F.col("g.avg_premium"))
    .select(
        "s.*",
        "g.avg_premium",
        F.round(F.col("s.premium") - F.col("g.avg_premium"), 2).alias("premium_minus_avg"),
    )
)

with_windows = (
    above_average
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "days_end_date_to_start_date",
        "product_clean",
        "start_date_label",
        "avg_premium",
        "premium_minus_avg",
        F.count(F.lit(1)).over(w_product).alias("rows_in_product"),
        F.row_number().over(w_status_by_days_end_date_to_s).alias("rn"),
    )
)

claim_stats = (
    df_claims
    .groupBy("policy_id")
    .agg(
        F.max("reported_at").alias("last_claim_reported_at"),
        F.count("*").alias("claim_count"),
        F.count("*").alias("claim_count_2"),
    )
)

with_stats = (
    with_windows.alias("s")
    .join(claim_stats.alias("c"), F.col("c.policy_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        "c.last_claim_reported_at",
        F.coalesce(F.col("c.claim_count"), F.lit(0)).alias("claim_count"),
        F.coalesce(F.col("c.claim_count_2"), F.lit(0)).alias("claim_count_2"),
    )
)

summary = (
    with_stats
    .groupBy("claim_count", "claim_count_2", "product")
    .agg(
        F.count("*").alias("row_count"),
        F.countDistinct("id").alias("distinct_ids"),
        F.array_sort(F.collect_set("product")).alias("product_list"),
        F.min("rn").alias("min_rn"),
    )
)

result = (
    summary
    .filter(F.col("product").isNotNull())
    .select(
        "claim_count",
        "claim_count_2",
        "product",
        "row_count",
        "distinct_ids",
        "product_list",
        F.coalesce(F.col("product"), F.lit("UNKNOWN")).alias("product_or_unknown"),
        F.coalesce(F.col("product"), F.lit("UNKNOWN")).alias("product_or_unknown_2"),
    )
)
