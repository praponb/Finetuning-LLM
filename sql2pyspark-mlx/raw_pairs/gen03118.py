from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "left")
    .groupBy(
        F.when(F.col("s.rating") >= 4.5, "high").when(F.col("s.rating") >= 1.6, "mid").otherwise("low").alias("rating_bucket"),
    )
    .agg(
        F.countDistinct(F.col("p.stock_qty")).alias("unique_brands"),
        F.min(F.col("p.launch_date")).alias("first_launch_date"),
    )
    .orderBy(F.col("rating_bucket").desc())
    .limit(5)
)
