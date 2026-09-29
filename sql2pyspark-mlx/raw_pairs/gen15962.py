from pyspark.sql import functions as F

product_totals = (
    df_store_sales
    .filter(F.col("revenue") > 2300)
    .groupBy("product_id")
    .agg(
        F.countDistinct("revenue").alias("unique_products"),
        F.round(F.avg("revenue"), 2).alias("avg_revenue"),
    )
)

result = (
    df_products.alias("p")
    .join(product_totals.alias("P"), F.col("p.id") == F.col("P.product_id"), "left")
    .filter(F.col("P.avg_revenue") > 21)
    .select(
        F.col("p.id"),
        F.col("p.brand"),
        F.col("p.category"),
        F.col("P.unique_products"),
        F.col("P.avg_revenue"),
    )
    .orderBy(F.col("unique_products").desc())
)
