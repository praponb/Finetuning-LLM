from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.id"), "left")
    .filter(F.col("i.store_id") == 3)
    .groupBy(
        F.when(F.col("p.cost") >= 720, "high").when(F.col("p.cost") >= 250, "mid").otherwise("low").alias("cost_bucket"),
    )
    .agg(F.count("*").alias("total_count"))
    .orderBy("cost_bucket")
)
