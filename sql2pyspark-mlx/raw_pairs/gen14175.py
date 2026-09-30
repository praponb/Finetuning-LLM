from pyspark.sql import functions as F

product_summary = (
    df_order_items
    .groupBy("product_id")
    .agg(F.sum("quantity").alias("total_quantity"))
)

result = (
    df_products.alias("p")
    .join(product_summary.alias("P"), F.col("p.id") == F.col("P.product_id"), "left")
    .filter(F.col("P.total_quantity") > 25)
    .select(
        F.col("p.id"),
        F.col("p.brand"),
        F.col("p.stock_qty"),
        F.col("P.total_quantity"),
    )
    .orderBy(F.col("stock_qty").desc())
)
