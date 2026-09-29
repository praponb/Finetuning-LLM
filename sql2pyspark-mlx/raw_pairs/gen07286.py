from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "inner")
    .filter(F.upper(F.col("p.product_name")).like("%APT%"))
    .groupBy(F.col("s.country"))
    .agg(
        F.avg(F.col("p.cost")).alias("avg_cost"),
        F.round(F.stddev(F.col("p.cost")), 2).alias("stddev_cost"),
    )
    .filter(F.col("stddev_cost") > 43)
    .orderBy("country")
)
