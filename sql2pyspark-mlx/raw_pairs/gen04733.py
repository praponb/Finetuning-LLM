from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("quantity", "id").rowsBetween(-2, Window.currentRow)
w2 = Window.partitionBy("product_id")

filtered = (
    df_order_items
    .filter(F.col("product_id").isNotNull() & (F.col("product_id") == 31))
    .select(
        "id",
        "product_id",
        "quantity",
        "unit_price",
        (F.col("quantity") * F.col("unit_price")).alias("line_total"),
    )
)

with_windows = (
    filtered
    .filter(F.col("line_total") < 65)
    .select(
        "id",
        "product_id",
        F.count("line_total").over(w).alias("last3_count_line_total"),
        F.min("line_total").over(w).alias("last3_min_line_total"),
        F.sum("line_total").over(w).alias("last3_sum_line_total"),
        F.corr("unit_price", "quantity").over(w2).alias("corr_unit_price_quantity"),
    )
)

base = (
    with_windows
    .filter(F.col("last3_min_line_total").between(24, 34))
    .select(
        "id",
        "product_id",
        "last3_count_line_total",
        "last3_min_line_total",
        "last3_sum_line_total",
        F.floor("last3_min_line_total").alias("last3_min_line_total_whole"),
    )
)

base_order_items = (
    base
    .filter(F.col("product_id") == 23)
    .select(
        "*",
        F.round((F.col("last3_min_line_total") * 1.15), 2).alias("adjusted_last3_min_line_total"),
    )
)

report = (
    base_order_items
    .select(
        "id",
        "last3_count_line_total",
        "last3_min_line_total",
        "last3_min_line_total_whole",
        "adjusted_last3_min_line_total",
    )
)

result = (
    report
    .select(
        "id",
        "last3_count_line_total",
        "last3_min_line_total",
        "last3_min_line_total_whole",
    )
    .orderBy(F.col("last3_count_line_total").asc_nulls_first())
)
