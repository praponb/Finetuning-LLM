from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("order_id").orderBy("unit_price", "id")
w_recent = Window.partitionBy("product_id").orderBy("quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("status").orderBy("amount", "id")

scored = (
    df_order_items
    .select(
        "*",
        F.last("unit_price", ignorenulls=True).over(w).alias("last_known_unit_price"),
        F.lead("quantity", 1).over(w2).alias("next_quantity"),
        F.last("unit_price", ignorenulls=True).over(w_recent).alias("last_known_unit_price_2"),
    )
)

with_dims = (
    scored
    .alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "inner")
    .select("s.*", F.col("o.shipping_fee"), F.col("o.status"), F.col("o.amount"))
)

ranked = (
    with_dims
    .withColumn("row_num", F.row_number().over(w4))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

result = (
    ranked
    .groupBy(
        F.when(F.col("last_known_unit_price_2") >= 380, "PREMIUM").otherwise("STANDARD").alias("last_known_unit_price_2_tier"),
    )
    .agg(
        F.percentile_approx("shipping_fee", 0.5).alias("shipping_fee_p50"),
        F.count(F.when(F.col("status") == "CANCELLED", 1)).alias("cancelled_count"),
        F.sum(F.when(F.col("status") == "PENDING", F.col("shipping_fee")).otherwise(0)).alias("pending_shipping_fee"),
    )
)
