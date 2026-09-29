from pyspark.sql import functions as F

product_totals = (
    df_order_items
    .groupBy("product_id")
    .agg(
        F.round(F.avg("unit_price"), 2).alias("avg_unit_price"),
        F.sum((F.col("quantity") * F.col("unit_price"))).alias("revenue"),
    )
)

result = (
    df_products.alias("p")
    .join(product_totals.alias("pt"), F.col("p.id") == F.col("pt.product_id"), "left")
    .filter(F.col("pt.revenue") > 29)
    .select(
        F.col("p.stock_qty"),
        F.col("p.cost"),
        F.col("pt.avg_unit_price"),
        F.col("pt.revenue"),
    )
)
