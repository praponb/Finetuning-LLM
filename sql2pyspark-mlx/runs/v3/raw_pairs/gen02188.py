from pyspark.sql import functions as F

result = (
    df_stores
    .filter((F.col("store_name").like("%Central%") & (F.col("opened_date") <= "2024-08-05")) | (F.col("sqft") == 8286))
    .groupBy("city")
    .agg(F.max("opened_date").alias("last_opened_date"))
    .orderBy("last_opened_date", "city")
    .limit(20)
)
