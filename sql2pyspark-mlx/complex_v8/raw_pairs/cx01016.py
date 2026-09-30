from pyspark.sql import functions as F

base = (
    df_policyholders
    .filter(F.col("segment") != "VIP")
    .select(
        "id",
        "holder_name",
        "risk_score",
        "joined_on",
        "segment",
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("joined_on")).alias("days_since_joined_on"),
    )
)

holder_rows = (
    base
    .select(
        "segment",
        "joined_on",
        F.col("holder_name").alias("label"),
        F.lit("holder").alias("src"),
    )
)

holder_side = (
    df_policyholders
    .filter(F.col("segment").isNotNull())
    .select(
        "region",
        "joined_on",
        "segment",
        F.lit("holder_2").alias("src"),
    )
)

result = (
    holder_rows
    .union(holder_side)
    .distinct()
    .orderBy(F.col("src").desc(), "label")
)
