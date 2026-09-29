from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("carrier") != "Kerry")
    .select(
        "shipped_date",
        "carrier",
        F.datediff(F.current_date(), "shipped_date").alias("days_since_shipped_date"),
        F.initcap(F.trim("status")).alias("status_title"),
    )
    .orderBy(F.col("shipped_date").desc())
)
