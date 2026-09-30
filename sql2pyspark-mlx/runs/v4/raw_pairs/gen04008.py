from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("method").orderBy(F.col("amount").desc_nulls_last(), "id")
quantity_by_order_id = df_order_items.groupBy("order_id").agg(F.sum("quantity").alias("sum_order_item_quantity"))

deduped = (
    df_payments
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
)

result = (
    deduped
    .join(quantity_by_order_id, on="order_id", how="left")
    .select("id", "order_id", "amount", "paid_at", F.col("sum_order_item_quantity"))
    .orderBy(F.col("sum_order_item_quantity").desc())
)
