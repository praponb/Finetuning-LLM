from pyspark.sql import functions as F

prepared_holders = (
    df_policyholders
    .filter((~F.col("region").isin("South", "Central")) & (F.col("joined_on") < F.lit("2025-09-02").cast("date")))
    .select(
        "id",
        "holder_name",
        "region",
        "risk_score",
        "joined_on",
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("joined_on")).alias("days_since_joined_on"),
    )
)

holder_rows = (
    prepared_holders
    .filter((F.col("days_since_joined_on") > 140) & (F.col("region") == "South"))
    .select("id", "joined_on")
)

holder_side = df_policyholders.select("id", "joined_on")

result = (
    holder_rows
    .union(holder_side)
    .orderBy("id")
)
