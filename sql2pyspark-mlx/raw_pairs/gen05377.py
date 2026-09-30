from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.id"), "left")
    .filter(F.col("i.product_id") == 7)
    .groupBy(
        F.col("p.supplier_id"),
        F.when(F.col("p.cost") >= 680, "high").when(F.col("p.cost") >= 150, "mid").otherwise("low").alias("cost_bucket"),
    )
    .agg(F.round(F.avg(F.col("i.quantity")), 2).alias("avg_quantity"))
    .filter(F.col("avg_quantity") > 62)
)
