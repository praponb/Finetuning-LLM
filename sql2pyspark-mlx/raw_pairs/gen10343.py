from pyspark.sql import functions as F

base = (
    df_stores
    .filter(F.col("store_name").like("%Kiosk%") & F.col("region").isNotNull() & (F.upper(F.trim("store_name")) == "RIVERSIDE"))
)

opened_month_summary = (
    base
    .groupBy(F.month("opened_date").alias("opened_month"))
    .agg(
        F.max("sqft").alias("max_sqft"),
        F.count("*").alias("cnt"),
    )
)

result = (
    opened_month_summary
    .select("opened_month", "max_sqft", "cnt")
    .orderBy(F.col("cnt").desc(), F.col("opened_month").desc())
    .limit(5)
)
