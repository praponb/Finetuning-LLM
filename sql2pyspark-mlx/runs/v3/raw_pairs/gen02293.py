from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_stores.alias("s"), F.col("i.store_id") == F.col("s.id"), "inner")
    .join(df_products.alias("p"), F.col("p.id") == F.col("i.product_id"), "inner")
    .filter(F.col("s.store_name").isNotNull())
    .groupBy(
        F.when(F.col("p.cost") >= 690, "high").when(F.col("p.cost") >= 200, "mid").otherwise("low").alias("cost_bucket"),
        F.col("p.supplier_id"),
    )
    .agg(F.avg(F.col("i.quantity")).alias("avg_quantity"))
    .filter(F.col("avg_quantity") > 190)
)
