from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("line_total", "id")
w2 = Window.partitionBy("product_id").orderBy("unit_price", "id").rowsBetween(-3, -1)

filtered = (
    df_order_items
    .filter(F.col("product_id").isNotNull())
    .select(
        "id",
        "order_id",
        "product_id",
        "unit_price",
        F.round((F.col("unit_price") / F.col("quantity")), 2).alias("line_total"),
    )
)

result = (
    filtered
    .select(
        "id",
        "order_id",
        "product_id",
        "unit_price",
        "line_total",
        F.lag("unit_price", 1, 0).over(w).alias("prev_unit_price"),
        F.avg("line_total").over(w2).alias("line_total_avg_3_prior"),
    )
)
