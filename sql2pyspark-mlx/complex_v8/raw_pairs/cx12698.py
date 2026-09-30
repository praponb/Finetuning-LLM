from pyspark.sql import functions as F

cleaned = (
    df_policies
    .filter((~F.col("status").isin("LAPSED", "EXPIRED")) & (F.col("premium") <= 3465.4))
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "end_date",
        "status",
        (F.floor(F.col("premium") / 1000) * 1000).alias("premium_bucket"),
        F.coalesce(F.col("status"), F.lit("UNKNOWN")).alias("status_or_unknown"),
        F.concat(F.col("product"), F.lit("#"), F.col("id").cast("string")).alias("product_ref"),
    )
)

policy_stats = (
    df_policies
    .groupBy("holder_id")
    .agg(
        F.max("start_date").alias("last_policy_start_date"),
        F.min("start_date").alias("first_policy_start_date"),
    )
)

summarized_rows = (
    cleaned.alias("s")
    .join(policy_stats.alias("p"), F.col("p.holder_id") == F.col("s.holder_id"), "left")
    .select("s.*", "p.last_policy_start_date", "p.first_policy_start_date")
)

policy_stats_2 = (
    df_policies
    .groupBy("holder_id")
    .agg(
        F.min("premium").alias("min_policy_premium"),
        F.min("end_date").alias("first_policy_end_date"),
        F.count(F.when(F.col("status") == "LAPSED", 1)).alias("policy_status_count"),
    )
)

with_stats = (
    summarized_rows.alias("s")
    .join(policy_stats_2.alias("p"), F.col("p.holder_id") == F.col("s.holder_id"), "left")
    .select(
        "s.*",
        "p.min_policy_premium",
        "p.first_policy_end_date",
        F.coalesce(F.col("p.policy_status_count"), F.lit(0)).alias("policy_status_count"),
    )
)

result = (
    with_stats
    .cube("product", "status")
    .agg(
        F.grouping_id().alias("grouping_level"),
        F.max("last_policy_start_date").alias("last_last_policy_start_date"),
        F.count("*").alias("cnt"),
        F.percentile_approx("premium", 0.5).alias("median_premium"),
    )
    .select(
        F.coalesce(F.col("product"), F.lit("ALL")).alias("product"),
        F.coalesce(F.col("status"), F.lit("ALL")).alias("status"),
        "grouping_level",
        "last_last_policy_start_date",
        "cnt",
        "median_premium",
    )
    .orderBy(F.col("product").asc_nulls_last(), F.col("status").asc_nulls_last())
)
