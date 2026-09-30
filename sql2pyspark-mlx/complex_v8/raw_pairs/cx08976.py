from pyspark.sql import functions as F

prepared_policys = (
    df_policies
    .filter((F.col("status") == "EXPIRED") & (F.col("holder_id").isNotNull()))
    .select(
        "id",
        "holder_id",
        "product",
        "start_date",
        "end_date",
        F.round(F.col("premium") * 0.9, 2).alias("premium_adj"),
    )
)

policy_rows = (
    prepared_policys
    .select(
        F.col("product").alias("label"),
        "id",
        F.col("start_date").alias("event_date"),
    )
)

claim_side = (
    df_claims
    .select(
        F.col("claim_type").alias("label"),
        "id",
        F.col("claim_date").alias("event_date"),
    )
)

stacked = (
    policy_rows
    .unionByName(claim_side)
    .distinct()
)

result = (
    stacked
    .groupBy("label")
    .agg(
        F.count("*").alias("n_rows"),
        F.sum("id").alias("total_id"),
        F.max("event_date").alias("latest_event_date"),
    )
    .orderBy("label")
)
