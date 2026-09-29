from pyspark.sql import functions as F

filtered = (
    df_policyholders
    .filter((F.col("joined_on") >= F.lit("2024-07-18").cast("date")) | (F.lower(F.col("holder_name")).like("%su%")))
    .select(
        "id",
        "holder_name",
        "risk_score",
        "segment",
        F.length(F.trim(F.col("segment"))).alias("segment_len"),
    )
)

holder_rows = filtered.select("segment")

holder_rows_2 = (
    df_policyholders
    .filter(F.col("risk_score") >= 0.3)
    .select("region")
)

result = holder_rows.exceptAll(holder_rows_2)
