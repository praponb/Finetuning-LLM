from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("line_total", "id")
w2 = Window.partitionBy("order_id", "product_id")
w3 = Window.partitionBy("product_id")
w4 = Window.partitionBy("product_id").orderBy(F.col("corr_unit_price_quantity").desc(), "id")

cleaned = (
    df_order_items
    .filter(F.col("product_id").isin([17, 33]))
    .select("*", (F.col("quantity") - F.col("unit_price")).alias("line_total"))
)

valid = (
    cleaned
    .filter(F.col("quantity") < 7)
    .select(
        "*",
        (F.col("unit_price") / F.nullif("line_total", F.lit(0))).alias("unit_price_per_line_total"),
        F.when(F.col("quantity") >= 8, "High").when(F.col("quantity") >= 2, "Medium").otherwise("Low").alias("quantity_level"),
    )
)

scored = (
    valid
    .withColumn("next_quantity", F.lead("quantity", 1).over(w))
    .withColumn("corr_unit_price_quantity", F.corr("unit_price", "quantity").over(w2))
    .withColumn("corr_quantity_unit_price", F.corr("quantity", "unit_price").over(w3))
)

ranked = (
    scored
    .withColumn("row_num", F.row_number().over(w4))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

joined = (
    ranked
    .alias("src")
    .join(df_orders.alias("b"), F.col("src.order_id") == F.col("b.id"), "left")
    .select("src.*", F.col("b.channel"), F.col("b.shipping_fee"), F.col("b.status"))
)

with_dims = (
    joined
    .alias("e")
    .join(df_products.alias("p"), F.col("e.product_id") == F.col("p.id"), "left")
    .select("e.*", F.col("p.product_name"), F.col("p.stock_qty"))
)

result = (
    with_dims
    .select(
        "id",
        "order_id",
        "product_id",
        "line_total",
        "quantity_level",
        "corr_unit_price_quantity",
        "shipping_fee",
        "status",
        "product_name",
    )
    .orderBy("corr_unit_price_quantity")
)
