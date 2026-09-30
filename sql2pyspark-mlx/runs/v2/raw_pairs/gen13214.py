from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.id"), "inner")
    .filter(F.col("p.cost") > 630)
    .groupBy(F.col("p.brand"))
    .agg(
        F.count("*").alias("num_inventory"),
        F.avg(F.col("i.quantity")).alias("avg_quantity"),
    )
    .orderBy(F.col("avg_quantity").desc())
)
