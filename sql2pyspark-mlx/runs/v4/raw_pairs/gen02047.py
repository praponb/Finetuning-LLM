from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("unit_price_minus_line_total", "id")

base = (
    df_order_items
    .filter(F.col("quantity") > 6)
    .select(
        "id",
        "order_id",
        "product_id",
        "quantity",
        "unit_price",
        (F.col("quantity") - F.col("unit_price")).alias("line_total"),
    )
)

filtered = (
    base
    .filter(F.col("product_id").isNotNull() & F.col("product_id").isin([1, 15, 25, 29]))
    .select(
        "id",
        "product_id",
        "unit_price",
        "line_total",
        F.round((F.col("quantity") * 0.9), 2).alias("quantity_adjusted"),
        (F.col("unit_price") - F.col("line_total")).alias("unit_price_minus_line_total"),
    )
)

latest = (
    filtered
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
    .drop("latest_rank")
)

result = (
    latest
    .select(
        "id",
        "product_id",
        "line_total",
        "quantity_adjusted",
        "unit_price_minus_line_total",
    )
    .orderBy(F.col("line_total").desc())
)
