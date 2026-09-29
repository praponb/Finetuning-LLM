from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.year("shipped_date") >= 2025)
    .select(
        "id",
        "carrier",
        F.initcap(F.trim("status")).alias("status_title"),
        F.datediff(F.current_date(), "shipped_date").alias("days_since_shipped_date"),
    )
    .orderBy("days_since_shipped_date", F.col("id").desc())
)
