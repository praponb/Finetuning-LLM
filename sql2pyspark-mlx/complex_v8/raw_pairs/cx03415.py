from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_holder_segment = Window.partitionBy("holder_segment")
w_status_by_start_date_prior_1 = Window.partitionBy("status").orderBy(F.col("start_date").desc(), "id").rowsBetween(-1, -1)

scoped = (
    df_policies
    .filter((F.col("premium").between(2013.8, 3719.7)) & (F.col("holder_id") <= 13))
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "end_date",
        "status",
        F.abs(F.col("premium") - 2289.0).alias("premium_distance"),
        F.when(F.col("product").isin("TRAVEL", "HEALTH"), "group_a").when(F.col("product") == "LIFE", "group_b").otherwise("other").alias("product_group"),
    )
)

tagged = (
    scoped
    .filter(F.col("premium") <= 3532.7)
    .select(
        "*",
        F.round(F.col("premium_distance") / F.nullif(F.col("premium"), F.lit(0)), 3).alias("premium_distance_per_premium"),
        F.length(F.trim(F.col("product_group"))).alias("product_group_len"),
        (F.col("premium_distance") + F.col("premium")).alias("premium_distance_plus_premium"),
    )
)

derived = (
    tagged
    .filter(F.col("premium") <= 3465.4)
    .select(
        "*",
        F.trunc(F.col("start_date"), "MM").alias("start_date_month_start"),
        F.length(F.trim(F.col("status"))).alias("status_len"),
    )
)

with_details = (
    derived.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "left")
    .select(
        "s.id",
        "s.holder_id",
        "s.product",
        "s.start_date",
        "s.end_date",
        "s.status",
        "s.product_group",
        "s.premium_distance_per_premium",
        "s.premium_distance_plus_premium",
        "s.start_date_month_start",
        "p.holder_name",
        F.col("p.segment").alias("holder_segment"),
        F.col("p.region").alias("holder_region"),
    )
)

windowed = (
    with_details
    .select(
        "*",
        F.round(F.col("premium_distance_per_premium") / F.nullif(F.sum("premium_distance_per_premium").over(w_holder_segment), F.lit(0)), 4).alias("premium_distance_per_premium_share_of_holder_seg"),
        F.round(F.avg("premium_distance_per_premium").over(w_status_by_start_date_prior_1), 2).alias("premium_distance_per_premium_avg_1_prior"),
    )
)

summary = (
    windowed
    .groupBy("holder_region")
    .agg(
        F.min("end_date").alias("first_end_date"),
        F.sum(F.when(F.col("holder_segment") == "VIP", F.col("premium_distance_per_premium")).otherwise(0)).alias("premium_distance_per_premium_holder_segment_vip"),
        F.min("start_date").alias("first_start_date"),
        F.round(F.sum("premium_distance_per_premium_avg_1_prior") / F.nullif(F.sum("premium_distance_per_premium_share_of_holder_seg"), F.lit(0)), 3).alias("premium_distance_per_premium_avg_1_prior_to_prem"),
        F.min("premium_distance_per_premium_avg_1_prior").alias("min_premium_distance_per_premium_avg_1_prior"),
        F.max("premium_distance_plus_premium").alias("max_premium_distance_plus_premium_check"),
    )
    .filter(F.col("max_premium_distance_plus_premium_check") > 2000.0)
    .drop("max_premium_distance_plus_premium_check")
)

result = (
    summary
    .select(
        "holder_region",
        "first_end_date",
        "premium_distance_per_premium_holder_segment_vip",
        "first_start_date",
        "premium_distance_per_premium_avg_1_prior_to_prem",
        "min_premium_distance_per_premium_avg_1_prior",
        F.substring(F.trim(F.col("holder_region")), 1, 2).alias("holder_region_prefix"),
        F.coalesce(F.col("holder_region"), F.lit("UNKNOWN")).alias("holder_region_or_unknown"),
    )
    .orderBy(F.col("holder_region_or_unknown").asc())
)
