from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_joined_on_ties = Window.orderBy(F.col("joined_on").asc_nulls_last())
w_all_by_joined_on = Window.orderBy(F.col("joined_on").asc_nulls_last(), "id")
w_all_by_joined_on_full = Window.orderBy(F.col("joined_on").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_dedup = Window.partitionBy("region").orderBy(F.col("last_policy_end_date").desc(), F.col("id").desc())
w_joined_on_rank_joined_on_den = Window.partitionBy("joined_on_rank", "joined_on_dense_rank")
w_region_joined_on_dense_rank = Window.partitionBy("region", "joined_on_dense_rank").orderBy("first_joined_on")
w_region_joined_on_dense_rank_2 = Window.partitionBy("region", "joined_on_dense_rank").orderBy("first_joined_on", "id").rowsBetween(-6, -1)

recent = (
    df_policyholders
    .filter((F.col("joined_on").isNotNull()) & (F.lower(F.col("holder_name")).like("%be%")))
    .select(
        "id",
        "holder_name",
        "region",
        "joined_on",
    )
)

ranked = (
    recent
    .select(
        "*",
        F.dense_rank().over(w_all_by_joined_on_ties).alias("joined_on_dense_rank"),
        F.rank().over(w_all_by_joined_on).alias("joined_on_rank"),
        F.first("joined_on").over(w_all_by_joined_on_full).alias("first_joined_on"),
    )
)

without_matches = (
    ranked.alias("s")
    .join(df_policies.alias("p"), (F.col("p.holder_id") == F.col("s.id")) & (F.year(F.col("p.end_date")) == 2025) & (F.col("p.product") == "HEALTH"), "left_semi")
)

policy_stats = (
    df_policies
    .groupBy("holder_id")
    .agg(
        F.count("*").alias("policy_count"),
        F.round(F.avg("premium"), 2).alias("avg_policy_premium"),
        F.max("end_date").alias("last_policy_end_date"),
    )
)

with_stats = (
    without_matches.alias("s")
    .join(policy_stats.alias("p"), F.col("p.holder_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("p.policy_count"), F.lit(0)).alias("policy_count"),
        "p.avg_policy_premium",
        "p.last_policy_end_date",
    )
)

unique_rows = (
    with_stats
    .withColumn("row_num", F.row_number().over(w_dedup))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

metrics = (
    unique_rows
    .select(
        "id",
        "holder_name",
        "region",
        "joined_on",
        "joined_on_dense_rank",
        "first_joined_on",
        "last_policy_end_date",
        F.round(F.col("joined_on_rank") / F.nullif(F.sum("joined_on_rank").over(w_joined_on_rank_joined_on_den), F.lit(0)), 4).alias("joined_on_rank_share_of_joined_on_rank_joined_on"),
        F.dense_rank().over(w_region_joined_on_dense_rank).alias("first_joined_on_dense_rank"),
        F.max("policy_count").over(w_region_joined_on_dense_rank_2).alias("policy_count_max_6_prior"),
        F.round(F.avg("avg_policy_premium").over(w_joined_on_rank_joined_on_den), 2).alias("joined_on_rank_joined_on_dense_rank_avg_avg_poli"),
    )
)

result = (
    metrics
    .select(
        "id",
        "holder_name",
        "joined_on",
        "joined_on_dense_rank",
        "first_joined_on",
        "last_policy_end_date",
        "joined_on_rank_share_of_joined_on_rank_joined_on",
        "first_joined_on_dense_rank",
        "policy_count_max_6_prior",
    )
)
