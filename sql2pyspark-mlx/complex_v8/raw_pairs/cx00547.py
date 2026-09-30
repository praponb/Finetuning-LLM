from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_distinct_status_status = Window.partitionBy("distinct_status", "status")
w_status_by_distinct_status_bu = Window.partitionBy("status").orderBy(F.col("distinct_status_bucket").desc())

valid = (
    df_policies
    .select(
        "id",
        "holder_id",
        "product",
        "end_date",
        "status",
        F.substring(F.trim(F.col("status")), 1, 2).alias("status_prefix"),
    )
)

rollup_base = (
    valid
    .groupBy("status")
    .agg(
        F.countDistinct("status").alias("distinct_status"),
        F.min("end_date").alias("first_end_date"),
    )
)

screened = (
    rollup_base
    .crossJoin(rollup_base.agg(F.avg("distinct_status").alias("overall_avg_distinct_status")))
    .filter(F.col("distinct_status") > F.col("overall_avg_distinct_status"))
    .drop("overall_avg_distinct_status")
)

enriched = (
    screened
    .select(
        "status",
        "distinct_status",
        "first_end_date",
        F.lower(F.trim(F.col("status"))).alias("status_clean"),
        F.when(F.col("distinct_status") >= 8, "large").when(F.col("distinct_status") >= 4, "medium").otherwise("small").alias("distinct_status_tier"),
        (F.floor(F.col("distinct_status") / 2) * 2).alias("distinct_status_bucket"),
    )
)

with_windows = (
    enriched
    .select(
        "*",
        F.size(F.collect_set("distinct_status_tier").over(w_distinct_status_status)).alias("distinct_distinct_status_tier_in_distinct_status"),
        F.count(F.lit(1)).over(w_distinct_status_status).alias("rows_in_distinct_status_status"),
        F.round(F.percent_rank().over(w_status_by_distinct_status_bu), 3).alias("distinct_status_bucket_percent_rank"),
    )
)

scored = (
    with_windows
    .filter(F.col("distinct_distinct_status_tier_in_distinct_status") > 3)
    .select(
        "*",
        F.when(F.col("status").isin("LAPSED", "ACTIVE"), "group_a").when(F.col("status") == "CANCELLED", "group_b").otherwise("other").alias("status_group"),
        F.round(F.col("distinct_status_bucket") / F.nullif(F.col("distinct_status"), F.lit(0)), 3).alias("distinct_status_bucket_per_distinct_status"),
    )
)

result = (
    scored
    .select(
        "status",
        "distinct_status",
        "first_end_date",
        "status_clean",
        "distinct_status_tier",
        "distinct_status_bucket",
        "distinct_distinct_status_tier_in_distinct_status",
        "rows_in_distinct_status_status",
        "distinct_status_bucket_percent_rank",
        "distinct_status_bucket_per_distinct_status",
        F.least(F.col("distinct_status"), F.col("distinct_status_bucket")).alias("least_distinct_status_distinct_status_bucket"),
        F.lower(F.regexp_replace(F.trim(F.col("status_clean")), " ", "_")).alias("status_clean_slug"),
    )
    .orderBy(F.col("distinct_status_bucket").desc_nulls_first())
)
