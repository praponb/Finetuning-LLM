from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.id"), "left")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "inner")
    .filter(F.col("s.country") == "TH")
    .groupBy(
        F.when(F.col("s.rating") >= 3.7, "high").when(F.col("s.rating") >= 1.9, "mid").otherwise("low").alias("rating_bucket"),
    )
    .agg(
        (F.max(F.col("i.quantity")) - F.min(F.col("i.quantity"))).alias("quantity_range"),
        F.avg(F.col("i.quantity")).alias("avg_quantity"),
    )
    .orderBy("rating_bucket", F.col("avg_quantity").desc())
    .limit(1)
)
