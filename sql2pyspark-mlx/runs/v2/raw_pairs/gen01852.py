from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.id"), "inner")
    .join(df_stores.alias("s"), F.col("i.store_id") == F.col("s.id"), "left")
    .filter(F.col("i.quantity") <= 195)
    .groupBy(
        F.when(F.col("p.cost") >= 690, "high").when(F.col("p.cost") >= 140, "mid").otherwise("low").alias("cost_bucket"),
    )
    .agg(
        (F.max(F.col("i.quantity")) - F.min(F.col("i.quantity"))).alias("quantity_range"),
        F.max(F.col("i.quantity")).alias("max_quantity"),
    )
    .orderBy(F.col("quantity_range").desc(), F.col("cost_bucket").desc())
)
