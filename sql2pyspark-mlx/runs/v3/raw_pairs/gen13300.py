from pyspark.sql import functions as F

filtered_products = (
    df_products
    .filter((F.col("brand") == "Stark") & (F.col("cost") < 360) & (F.col("stock_qty") <= 391))
    .withColumn("category_title", F.initcap(F.trim("category")))
)

products_stats = (
    filtered_products
    .groupBy("brand")
    .agg(
        F.count("cost").alias("cost_count"),
        F.max("stock_qty").alias("max_stock_qty"),
    )
)

result = (
    products_stats
    .filter(F.col("cost_count") > 4)
    .select("brand", "cost_count", "max_stock_qty")
    .orderBy("brand", F.col("cost_count").desc())
    .limit(5)
)
