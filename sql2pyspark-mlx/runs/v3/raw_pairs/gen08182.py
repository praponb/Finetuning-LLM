from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_stores.alias("s"), F.col("i.store_id") == F.col("s.id"), "inner")
    .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.id"), "left")
    .groupBy(
        F.date_trunc("MONTH", F.col("p.launch_date")).alias("launch_month"),
        F.when(F.col("p.cost") >= 800, "high").when(F.col("p.cost") >= 320, "mid").otherwise("low").alias("cost_bucket"),
    )
    .agg(
        F.count("*").alias("row_count"),
        F.countDistinct(F.col("i.quantity")).alias("unique_products"),
    )
)
