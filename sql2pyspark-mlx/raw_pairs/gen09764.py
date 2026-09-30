from pyspark.sql import functions as F

result = (
    df_products
    .alias("e")
    .join(df_suppliers.alias("p"), F.col("e.supplier_id") == F.col("p.id"), "inner")
    .groupBy(
        F.col("p.country").alias("country"),
        F.col("p.city").alias("city"),
        F.col("e.category").alias("category"),
    )
    .agg(
        F.count("*").alias("rows_total"),
        F.round(F.avg(F.col("e.price")), 2).alias("avg_price"),
    )
)
