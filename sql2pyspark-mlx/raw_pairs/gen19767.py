from pyspark.sql import functions as F

result = (
    df_products
    .alias("e")
    .join(df_suppliers.alias("s"), F.col("e.supplier_id") == F.col("s.id"), "inner")
    .groupBy(F.col("s.country").alias("country"), F.col("e.category").alias("category"))
    .agg(
        F.count("*").alias("cnt"),
        F.avg(F.col("e.cost")).alias("avg_cost"),
        F.min(F.col("e.price")).alias("min_price"),
    )
)
