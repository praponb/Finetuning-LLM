from pyspark.sql import functions as F

product_totals = (
    df_store_sales
    .filter(F.col("store_id").isin([2, 3, 5, 6]))
    .groupBy("product_id")
    .agg(
        F.sum("revenue").alias("total_revenue"),
        F.min("units").alias("min_units"),
    )
)

result = (
    df_products.alias("p")
    .join(product_totals.alias("t"), F.col("p.id") == F.col("t.product_id"), "left")
    .filter(F.col("t.total_revenue") >= 11000)
    .select(
        F.col("p.id"),
        F.col("p.category"),
        F.col("t.total_revenue"),
        F.col("t.min_units"),
    )
    .orderBy("total_revenue")
)
