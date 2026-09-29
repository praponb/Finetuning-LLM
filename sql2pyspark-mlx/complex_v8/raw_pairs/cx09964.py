from pyspark.sql import functions as F

recent = (
    df_policyholders
    .filter(F.col("holder_name").isNotNull())
    .select(
        "id",
        "holder_name",
        "region",
        "joined_on",
        "segment",
    )
)

holder_side = (
    df_policyholders
    .filter(F.year(F.col("joined_on")) == 2024)
    .select(
        F.lit("holder").alias("src"),
        "id",
        F.col("region").alias("name"),
        F.col("risk_score").alias("value"),
    )
)

holder_rows = (
    df_policyholders
    .filter(~F.col("region").isin("West", "South"))
    .select(
        F.lit("holder_2").alias("src"),
        "id",
        F.col("segment").alias("name"),
        F.col("risk_score").alias("value"),
    )
)

result = holder_side.unionByName(holder_rows)
