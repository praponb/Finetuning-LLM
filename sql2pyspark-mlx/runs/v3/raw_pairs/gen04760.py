from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.id"), "inner")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "left")
    .groupBy(
        F.when(F.col("p.cost") >= 680, "high").when(F.col("p.cost") >= 130, "mid").otherwise("low").alias("cost_bucket"),
        F.col("p.category"),
    )
    .agg(
        F.sum(F.col("i.quantity")).alias("total_quantity"),
        F.count("*").alias("cnt"),
    )
)
