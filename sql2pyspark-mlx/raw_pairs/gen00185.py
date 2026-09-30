from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id")
w2 = Window.partitionBy("order_id").orderBy(F.col("paid_at").cast("bigint")).rangeBetween(-2592000, Window.currentRow)
w3 = Window.partitionBy(F.to_date("paid_at"), "status").orderBy("paid_at", "id")
quantity_by_order_id = df_order_items.groupBy("order_id").agg(F.sum("quantity").alias("sum_order_item_quantity"))

enriched = (
    df_payments
    .select(
        "id",
        "order_id",
        "method",
        "amount",
        "paid_at",
        "status",
        F.round((F.col("amount") / F.sum("amount").over(w)), 4).alias("amount_share"),
        F.avg("amount").over(w2).alias("amount_avg_30d"),
        F.lag("amount", 1).over(w3).alias("prev_amount"),
    )
)

result = (
    enriched
    .join(quantity_by_order_id, on="order_id", how="left")
    .select(
        "id",
        "order_id",
        "method",
        "amount",
        "amount_avg_30d",
        F.col("sum_order_item_quantity"),
    )
    .orderBy(F.col("method").asc_nulls_first())
)
