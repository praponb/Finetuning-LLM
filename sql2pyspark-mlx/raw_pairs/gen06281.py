from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy("product_id").orderBy("quantity", "id")
w_ordered = Window.orderBy("quantity", "id")
amount_by_order_id = df_payments.groupBy("order_id").agg(F.max("amount").alias("max_payment_amount"))

with_windows = (
    df_order_items
    .withColumn("prev_unit_price", F.lag("unit_price").over(w_recent))
    .withColumn("next_unit_price", F.lead("unit_price").over(w_recent))
    .withColumn("next_quantity", F.lead("quantity").over(w_recent))
    .withColumn("prev_quantity", F.lag("quantity").over(w_recent))
    .withColumn("event_seq", F.row_number().over(w_recent))
    .withColumn("next_unit_price_2", F.lead("unit_price", 1).over(w_ordered))
)

result = (
    with_windows
    .join(amount_by_order_id, on="order_id", how="left")
    .select(
        "id",
        "order_id",
        "product_id",
        "quantity",
        "prev_unit_price",
        "next_unit_price",
        "next_quantity",
        "event_seq",
        "next_unit_price_2",
        F.col("max_payment_amount"),
    )
    .orderBy("id")
)
