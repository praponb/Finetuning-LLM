from pyspark.sql import functions as F

cleaned_policys = (
    df_policies
    .filter(F.col("end_date").isNotNull())
    .select(
        "id",
        "holder_id",
        "premium",
        "start_date",
        "end_date",
        "status",
    )
)

enriched = (
    cleaned_policys
    .filter(F.col("holder_id").isNotNull())
    .select(
        "*",
        (F.floor(F.col("premium") / 1000) * 1000).alias("premium_bucket"),
        F.length(F.trim(F.col("status"))).alias("status_len"),
    )
)

grouped = (
    enriched
    .filter(F.col("premium") < 4417.0)
    .groupBy(F.date_format("end_date", "yyyy-MM").alias("end_date_ym"), "status")
    .agg(
        F.round(F.sum("premium"), 2).alias("total_premium"),
        F.countDistinct("holder_id").alias("distinct_holder_ids"),
        F.round(F.sum("status_len") / F.nullif(F.sum("premium"), F.lit(0)), 3).alias("status_len_to_premium_ratio"),
    )
)

labeled = grouped.withColumn("distinct_holder_ids_distance", F.abs(F.col("distinct_holder_ids") - 6))

tagged = (
    labeled
    .select(
        "*",
        F.substring(F.trim(F.col("status")), 1, 4).alias("status_prefix"),
        F.round(F.col("total_premium") / F.nullif(F.col("distinct_holder_ids"), F.lit(0)), 3).alias("total_premium_per_distinct_holder_ids"),
    )
)

tagged_2 = (
    tagged
    .select(
        "end_date_ym",
        "status",
        "total_premium",
        "status_len_to_premium_ratio",
        "distinct_holder_ids_distance",
        "status_prefix",
        "total_premium_per_distinct_holder_ids",
        F.lower(F.regexp_replace(F.trim(F.col("status_prefix")), " ", "_")).alias("status_prefix_slug"),
    )
)

result = (
    tagged_2
    .filter(F.col("distinct_holder_ids_distance") <= 9)
    .select(
        "end_date_ym",
        "status",
        "distinct_holder_ids_distance",
        "status_prefix",
        "total_premium_per_distinct_holder_ids",
        "status_prefix_slug",
        F.initcap(F.trim(F.col("status"))).alias("status_clean"),
    )
)
