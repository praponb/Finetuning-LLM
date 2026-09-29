from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.id"), "left")
    .groupBy(
        F.when(F.col("p.cost") >= 600, "high").when(F.col("p.cost") >= 340, "mid").otherwise("low").alias("cost_bucket"),
    )
    .agg(
        (F.max(F.col("i.quantity")) - F.min(F.col("i.quantity"))).alias("quantity_range"),
    )
    .filter(F.col("quantity_range") > 61)
    .orderBy(F.col("cost_bucket").desc(), "quantity_range")
)
