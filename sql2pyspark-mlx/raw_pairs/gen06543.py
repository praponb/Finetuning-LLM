from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("discount").asc_nulls_last(), "id")
amount_by_store_id = df_orders.groupBy("store_id").agg(F.avg("amount").alias("avg_order_amount"))

latest = (
    df_orders
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
    .select("id", "customer_id", "store_id", "shipping_fee")
)

result = (
    latest
    .join(amount_by_store_id, on="store_id", how="left")
    .select("id", "customer_id", "store_id", "shipping_fee", F.col("avg_order_amount"))
    .orderBy(F.col("id").asc_nulls_first())
)
