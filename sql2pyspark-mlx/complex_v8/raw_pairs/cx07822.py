from pyspark.sql import functions as F

prepared_holders = (
    df_policyholders
    .filter((F.col("region") == "South") & (F.col("segment").isin("VIP", "SME")))
    .select(
        "id",
        "region",
        "risk_score",
        "joined_on",
        "segment",
        F.date_trunc("year", F.col("joined_on")).alias("joined_on_year"),
    )
)

holder_side = (
    prepared_holders
    .filter((~F.col("region").isin("North", "Central")) & (F.col("risk_score") <= 0.6))
    .select(F.col("segment").alias("label"), "region")
)

policy_rows = (
    df_policies
    .filter((F.col("holder_id").isNotNull()) & (F.col("product") == "HEALTH"))
    .select(F.col("product").alias("label"), F.col("status").alias("region"))
)

unioned = holder_side.unionByName(policy_rows)

result = (
    unioned
    .groupBy("label")
    .agg(F.count("*").alias("n_rows"))
    .orderBy("label")
)
