from pyspark.sql import functions as F

prepared_holders = (
    df_policyholders
    .filter(F.col("joined_on") >= F.lit("2024-06-18").cast("date"))
    .select(
        "id",
        "holder_name",
        "risk_score",
        "segment",
        F.date_format(F.col("joined_on"), "yyyy-MM").alias("joined_on_label"),
    )
)

holder_rows = prepared_holders.select("id")

adjuster_side = df_adjusters.select("level")

result = (
    holder_rows
    .intersect(adjuster_side)
    .orderBy("id")
)
