from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy(F.year("order_date").alias("order_year"), "store_id")
    .agg(F.sum("amount").alias("total_amount"))
    .filter(F.col("total_amount") < 5200)
)
