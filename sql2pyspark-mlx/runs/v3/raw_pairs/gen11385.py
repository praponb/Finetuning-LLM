from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("p.id") == F.col("ss.product_id"), "left")
    .filter(F.array_contains(F.col("p.tags"), "premium"))
    .groupBy(F.col("p.category"))
    .agg(
        F.avg(F.col("ss.units")).alias("avg_units"),
        F.max(F.col("ss.revenue")).alias("max_revenue"),
    )
    .orderBy("category")
)
