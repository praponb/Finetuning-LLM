from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("p.id") == F.col("i.product_id"), "inner")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "inner")
    .groupBy(
        F.when(F.col("s.rating") >= 3.6, "high").when(F.col("s.rating") >= 2.0, "mid").otherwise("low").alias("rating_bucket"),
    )
    .agg(F.sum(F.col("i.quantity")).alias("total_quantity"))
    .orderBy("total_quantity")
)
