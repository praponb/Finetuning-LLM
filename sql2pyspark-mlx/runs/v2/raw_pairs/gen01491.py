from pyspark.sql import functions as F

result = (
    df_stores
    .filter((F.lower(F.trim("store_name")) == "airport kiosk") & (F.year("opened_date") >= 2024) & F.col("store_name").isNotNull())
    .rollup("region", F.month("opened_date").alias("opened_month"))
    .agg(F.min("opened_date").alias("min_opened_date"))
    .orderBy(F.col("region").desc())
)
