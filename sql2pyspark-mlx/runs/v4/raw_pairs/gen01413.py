from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.orderBy("line_total", "id")
w2 = Window.partitionBy("product_id").orderBy(F.col("unit_price").desc(), "id")
w3 = Window.orderBy("unit_price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_order_items
    .filter(F.col("quantity").isNotNull())
    .withColumn("line_total", (F.col("quantity") - F.col("unit_price")))
)

scored = (
    valid
    .select(
        "*",
        F.ntile(10).over(w_ordered).alias("line_total_bucket"),
        F.ntile(4).over(w2).alias("unit_price_bucket"),
        F.first("unit_price", ignorenulls=True).over(w3).alias("first_known_unit_price"),
    )
)

filtered = (
    scored
    .filter(F.col("product_id").isNotNull() & (F.col("order_id") == 31))
    .withColumn("quantity_num", F.col("quantity").cast("double"))
)

enriched = (
    filtered
    .alias("src")
    .join(df_products.alias("b"), F.col("src.product_id") == F.col("b.id"), "left")
    .select("src.*", F.col("b.brand"))
)

result = (
    enriched
    .select(
        "id",
        "order_id",
        "product_id",
        "quantity",
        "unit_price",
        "line_total",
        "unit_price_bucket",
        "first_known_unit_price",
    )
    .orderBy("first_known_unit_price")
)
