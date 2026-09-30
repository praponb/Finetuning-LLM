from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id", "product_id").orderBy("unit_price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
amount_by_order_id = df_payments.groupBy("order_id").agg(F.min("amount").alias("min_payment_amount"))

enriched = (
    df_order_items
    .filter(F.col("product_id").isNull())
    .withColumn("running_sum_quantity", F.sum("quantity").over(w))
    .withColumn("running_avg_quantity", F.avg("quantity").over(w))
    .withColumn("running_min_quantity", F.min("quantity").over(w))
)

result = (
    enriched
    .join(amount_by_order_id, on="order_id", how="left")
    .select(
        "id",
        "order_id",
        "quantity",
        "unit_price",
        "running_min_quantity",
        F.col("min_payment_amount"),
    )
)
