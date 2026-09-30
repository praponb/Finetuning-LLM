from pyspark.sql import functions as F

scoped_holders = (
    df_policyholders
    .filter(F.year(F.col("joined_on")) == 2025)
    .select(
        "id",
        "holder_name",
        "region",
        "risk_score",
        "joined_on",
    )
)

holder_rows = (
    scoped_holders
    .filter((F.col("joined_on") >= F.lit("2024-06-18").cast("date")) & (F.col("risk_score") >= 0.2))
    .select("holder_name", F.col("risk_score").alias("amount"))
)

holder_side = (
    df_policyholders
    .select(
        F.col("segment").alias("holder_name"),
        F.col("risk_score").alias("amount"),
    )
)

unioned = holder_rows.unionByName(holder_side)

result = (
    unioned
    .groupBy("holder_name")
    .agg(F.count("*").alias("n_rows"), F.sum("amount").alias("total_amount"))
    .orderBy("holder_name")
)
