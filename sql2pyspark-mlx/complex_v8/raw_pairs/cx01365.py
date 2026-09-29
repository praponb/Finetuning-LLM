from pyspark.sql import functions as F

prepared_policys = (
    df_policies
    .filter((F.col("holder_id") >= 19) & (F.col("status") == "CANCELLED"))
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "status",
        F.length(F.trim(F.col("status"))).alias("status_len"),
        (F.floor(F.col("premium") / 1000) * 1000).alias("premium_bucket"),
        F.date_trunc("year", F.col("start_date")).alias("start_date_year"),
    )
)

with_activity = (
    prepared_policys
    .crossJoin(prepared_policys.agg(F.min("premium_bucket").alias("overall_min_premium_bucket")))
    .filter(F.col("premium_bucket") > F.col("overall_min_premium_bucket"))
    .drop("overall_min_premium_bucket")
)

filtered = (
    with_activity.alias("s")
    .join(df_claims.alias("c"), F.col("c.policy_id") == F.col("s.id"), "left_semi")
    .filter(F.col("premium") > 2111.3)
)

result = (
    filtered
    .filter(F.col("status") == "ACTIVE")
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "status_len",
        "premium_bucket",
        "start_date_year",
        F.date_trunc("year", F.col("start_date_year")).alias("start_date_year_year"),
        F.abs(F.col("status_len") - 7).alias("status_len_distance"),
    )
    .orderBy(F.col("premium_bucket").desc_nulls_first(), F.col("status_len").desc())
)
