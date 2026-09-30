from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy(F.col("amount").asc_nulls_last(), "id")
quantity_by_order_id = df_order_items.groupBy("order_id").agg(F.avg("quantity").alias("avg_order_item_quantity"))

latest = (
    df_payments
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
    .select("id", "order_id", "paid_at")
)

result = (
    latest
    .join(quantity_by_order_id, on="order_id", how="left")
    .select("id", "order_id", F.col("avg_order_item_quantity"))
    .orderBy("id")
)
