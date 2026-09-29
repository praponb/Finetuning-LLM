from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.id"), "left")
    .filter(~F.col("p.category").isin(["Electronics", "Books", "Sports"]))
    .groupBy(
        F.date_trunc("MONTH", F.col("p.launch_date")).alias("launch_month"),
        F.when(F.col("p.price") >= 1140, "high").when(F.col("p.price") >= 480, "mid").otherwise("low").alias("price_bucket"),
    )
    .agg(
        F.min(F.col("i.quantity")).alias("min_quantity"),
        F.max(F.col("i.quantity")).alias("max_quantity"),
    )
    .orderBy("min_quantity")
)
