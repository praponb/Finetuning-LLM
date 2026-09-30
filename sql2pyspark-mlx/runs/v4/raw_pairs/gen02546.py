from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_products.alias("p"), F.col("oi.product_id") == F.col("p.id"), "inner")
    .filter(F.col("p.category") == "Fashion")
    .groupBy(
        F.col("p.brand"),
        F.when(F.col("p.cost") >= 770, "high").when(F.col("p.cost") >= 100, "mid").otherwise("low").alias("cost_bucket"),
    )
    .agg(F.min(F.col("oi.unit_price")).alias("min_unit_price"))
    .orderBy(F.col("cost_bucket").desc(), "brand")
)
