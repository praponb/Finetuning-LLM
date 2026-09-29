from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_region_region_len = Window.partitionBy("region", "region_len")
w_joined_on_month_region_by_re = Window.partitionBy(F.month("joined_on"), "region").orderBy(F.col("region_len").asc_nulls_last(), "id")
w_joined_on_month_region_by_re_2 = Window.partitionBy(F.month("joined_on"), "region").orderBy(F.col("region_len").asc_nulls_last(), "id").rowsBetween(-1, Window.currentRow)
w_joined_on_month_region_by_re_3 = Window.partitionBy(F.month("joined_on"), "region").orderBy(F.col("region_len").asc_nulls_last(), "id").rowsBetween(-3, -1)
w_joined_on_month_region_by_re_4 = Window.partitionBy(F.month("joined_on"), "region").orderBy(F.col("region_len").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

recent_holders = (
    df_policyholders
    .filter(F.col("joined_on").between(F.lit("2024-03-22").cast("date"), F.lit("2025-07-03").cast("date")))
    .select(
        "id",
        "holder_name",
        "region",
        "joined_on",
        F.concat(F.col("region"), F.lit("#"), F.col("id").cast("string")).alias("region_ref"),
    )
)

scored = (
    recent_holders
    .select(
        "*",
        F.concat_ws(" / ", F.col("region"), F.col("holder_name")).alias("region_holder_name_label"),
        F.length(F.trim(F.col("region"))).alias("region_len"),
    )
)

ranked = (
    scored
    .select(
        "id",
        "region",
        "region_holder_name_label",
        "region_len",
        F.round(F.col("region_len") / F.nullif(F.sum("region_len").over(w_region_region_len), F.lit(0)), 4).alias("region_len_share_of_region_region_len"),
        F.dense_rank().over(w_joined_on_month_region_by_re).alias("region_len_dense_rank"),
        F.sum("region_len").over(w_joined_on_month_region_by_re_2).alias("region_len_sum_2"),
        F.max("region_len").over(w_joined_on_month_region_by_re_3).alias("region_len_max_3_prior"),
        F.lag("region_len", 2, 0).over(w_joined_on_month_region_by_re).alias("prev2_region_len"),
        F.last("region_len").over(w_joined_on_month_region_by_re_4).alias("last_region_len"),
    )
)

policy_stats = (
    df_policies
    .groupBy("holder_id")
    .agg(F.count("*").alias("policy_count"))
)

with_stats = (
    ranked.alias("s")
    .join(policy_stats.alias("p"), F.col("p.holder_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("p.policy_count"), F.lit(0)).alias("policy_count"),
    )
)

result = (
    with_stats
    .select(
        "id",
        "region",
        "region_holder_name_label",
        "region_len",
        "region_len_sum_2",
        "region_len_max_3_prior",
        "prev2_region_len",
        "last_region_len",
    )
    .orderBy(F.col("id").desc())
    .limit(5)
)
