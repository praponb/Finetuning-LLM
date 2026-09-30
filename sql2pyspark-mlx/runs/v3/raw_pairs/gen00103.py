from pyspark.sql import functions as F

result = (
    df_suppliers.alias("s")
    .join(df_products.alias("p"), F.col("s.id") == F.col("p.supplier_id"), "left")
    .groupBy(F.col("s.id"), F.col("s.country"))
    .agg(
        F.count(F.col("p.id")).alias("product_count"),
        F.round(F.stddev(F.col("p.cost")), 2).alias("stddev_cost"),
    )
    .filter(F.col("stddev_cost") > 52)
    .orderBy("stddev_cost", F.col("id").desc())
)
