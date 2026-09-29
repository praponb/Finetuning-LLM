from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.quantity").desc(), F.col("c.id").desc())
units_by_store_id = df_store_sales.groupBy("store_id").agg(F.min("units").alias("min_store_sale_units"))

with_last = (
    df_orders
    .alias("s")
    .join(df_order_items.alias("c"), (F.col("c.order_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.store_id",
        "s.discount",
        "s.shipping_fee",
        F.col("c.unit_price").alias("order_item_unit_price"),
    )
)

result = (
    with_last
    .join(units_by_store_id, on="store_id", how="left")
    .select("id", "store_id", "shipping_fee", F.col("min_store_sale_units"))
    .orderBy(F.col("shipping_fee").desc(), "store_id")
)
