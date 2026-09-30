from pyspark.sql import functions as F

product_totals = (
    df_store_sales
    .filter(F.col("store_id").isin([1, 6]))
    .groupBy("product_id")
    .agg(
        F.min("revenue").alias("min_revenue"),
        F.avg("units").alias("avg_units"),
    )
)

result = (
    df_products.alias("p")
    .join(product_totals.alias("P"), F.col("p.id") == F.col("P.product_id"), "left")
    .filter(F.col("P.min_revenue") >= 3800)
    .select(
        F.col("p.id"),
        F.col("p.category"),
        F.col("P.min_revenue"),
        F.col("P.avg_units"),
    )
)
