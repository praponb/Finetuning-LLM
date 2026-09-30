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
    .join(product_totals.alias("P"), F.col("p.id") == F.col("P.product_id"), "left")
    .filter(F.col("P.avg_unit_price") >= 24)
    .select(
        F.col("p.id"),
        F.col("p.cost"),
        F.col("p.stock_qty"),
        F.col("P.avg_unit_price"),
        F.col("P.revenue"),
    )
    .orderBy(F.col("stock_qty").desc())
)
