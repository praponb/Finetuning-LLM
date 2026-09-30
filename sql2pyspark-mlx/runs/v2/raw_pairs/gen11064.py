from pyspark.sql import functions as F

recent_store_sales = (
    df_store_sales
    .filter(F.col("revenue") > 2700)
    .withColumn("revenue_int", F.col("revenue").cast("int"))
)

summary = (
    recent_store_sales
    .groupBy("store_id")
    .agg(F.max("sale_date").alias("latest_sale_date"))
)

result = (
    summary
    .select("store_id", "latest_sale_date")
    .orderBy("latest_sale_date")
)
