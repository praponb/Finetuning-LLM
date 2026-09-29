from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_stores.alias("s"), F.col("ss.store_id") == F.col("s.id"), "left")
    .filter(F.col("s.sqft").between(9411, 10462))
    .groupBy(F.col("s.city"), F.col("s.region"))
    .agg(F.count("*").alias("total_count"))
    .orderBy("total_count")
)
