from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("store_name").like("Sia%") & F.col("opened_date").between("2023-10-01", "2024-03-01"))
    .groupBy("city", "region")
    .agg(F.min("opened_date").alias("min_opened_date"))
    .orderBy("city", "region")
    .limit(5)
)
