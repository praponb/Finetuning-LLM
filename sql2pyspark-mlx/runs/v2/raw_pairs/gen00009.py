from pyspark.sql import functions as F

recent_products = (
    df_products
    .filter((F.col("launch_date") < "2025-03-23") | ~F.col("brand").isin(["Acme", "Stark", "Umbrella"]))
    .withColumn("cost_per_stock_qty", (F.col("cost") / F.nullif("stock_qty", F.lit(0))))
)

products_stats = (
    recent_products
    .groupBy(
        F.when(F.col("stock_qty") >= 388, "high").when(F.col("stock_qty") >= 75, "mid").otherwise("low").alias("stock_qty_bucket"),
    )
    .agg(F.concat_ws(", ", F.sort_array(F.collect_set("brand"))).alias("brand_values"))
)

result = products_stats.select("stock_qty_bucket", "brand_values")
