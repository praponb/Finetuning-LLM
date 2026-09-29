from pyspark.sql import functions as F

prepared = (
    df_policyholders
    .filter((F.col("segment").isin("VIP", "RETAIL")) & (F.col("region") == "North"))
    .select(
        "id",
        "holder_name",
        "region",
        "risk_score",
        "joined_on",
        F.when(F.col("risk_score") >= 0.6, "HIGH").when(F.col("risk_score") >= 0.3, "MEDIUM").otherwise("LOW").alias("risk_score_tier"),
    )
)

holder_rows = prepared.select("id")

holder_side = (
    df_policyholders
    .filter(F.col("region") != "East")
    .select("id")
)

result = holder_rows.intersect(holder_side)
