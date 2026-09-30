from pyspark.sql import functions as F

product_summary = (
    df_store_sales
    .filter(F.col("store_id").isin([1, 2, 4, 5]))
    .groupBy("product_id")
    .agg(
        F.round(F.avg("units"), 2).alias("avg_units"),
        F.round(F.avg("revenue"), 2).alias("avg_revenue"),
    )
)

result = (
    df_products.alias("p")
    .join(product_summary.alias("P"), F.col("p.id") == F.col("P.product_id"), "left")
    .select(
        F.col("p.cost"),
        F.col("p.product_name"),
        F.col("P.avg_units"),
        F.col("P.avg_revenue"),
    )
    .orderBy("product_name", F.col("avg_units").desc())
)
