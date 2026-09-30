from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("product_id").isin([5, 8]))
    .select("sale_date")
    .unionByName(df_subscriptions.select(F.col("start_date").alias("sale_date")))
)
