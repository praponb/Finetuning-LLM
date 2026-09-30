from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("amount").desc(), "id")
w_ordered = Window.orderBy("amount", "id").rowsBetween(-3, -1)
w_running = Window.partitionBy("order_id").orderBy("dedup_rank", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("product_id", "order_id").orderBy("shipping_fee", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("product_id", "order_id").orderBy("dedup_rank", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("product_id")
w7 = Window.partitionBy("order_id").orderBy(F.col("running_quantity").desc_nulls_first(), "id")

enriched = (
    df_order_items
    .alias("src")
    .join(df_orders.alias("b"), F.col("src.order_id") == F.col("b.id"), "left")
    .select(
        "src.id",
        "src.order_id",
        "src.product_id",
        "src.quantity",
        F.col("b.shipping_fee"),
        F.col("b.amount"),
    )
)

latest = (
    enriched
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
)

with_windows = (
    latest
    .select(
        "*",
        F.avg("quantity").over(w_ordered).alias("quantity_avg_3_prior"),
        F.sum("quantity").over(w_running).alias("running_quantity"),
        F.last("shipping_fee", ignorenulls=True).over(w4).alias("last_known_shipping_fee"),
        F.first("quantity", ignorenulls=True).over(w_running).alias("first_known_quantity"),
    )
)

scored = (
    with_windows
    .filter(F.col("running_quantity") == 15)
    .select(
        "*",
        F.first("first_known_quantity", ignorenulls=True).over(w5).alias("first_known_first_known_quantity"),
        F.round((F.col("quantity") / F.sum("quantity").over(w6)), 4).alias("quantity_share"),
    )
)

deduped = (
    scored
    .withColumn("row_num", F.row_number().over(w7))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

result = (
    deduped
    .select(
        "id",
        "order_id",
        "product_id",
        "quantity",
        "dedup_rank",
        "quantity_avg_3_prior",
        "last_known_shipping_fee",
        "first_known_quantity",
        "first_known_first_known_quantity",
    )
    .orderBy("quantity_avg_3_prior")
)
