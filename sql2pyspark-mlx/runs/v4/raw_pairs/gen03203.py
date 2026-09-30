from pyspark.sql import functions as F

product_stats = (
    df_store_sales
    .filter(F.col("units").between(12, 32))
    .groupBy("product_id")
    .agg(
        (F.max("units") - F.min("units")).alias("units_range"),
        F.round(F.avg("units"), 2).alias("avg_units"),
    )
)

result = (
    df_products.alias("p")
    .join(product_stats.alias("P"), F.col("p.id") == F.col("P.product_id"), "left")
    .filter(F.col("P.avg_units") >= 54)
    .select(
        F.col("p.id"),
        F.col("p.product_name"),
        F.col("p.supplier_id"),
        F.col("P.units_range"),
        F.col("P.avg_units"),
    )
    .orderBy(F.col("id").desc())
)
