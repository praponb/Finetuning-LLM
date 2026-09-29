from pyspark.sql import functions as F

filtered_products = (
    df_products
    .filter(F.col("brand") != "Globex")
    .withColumn("stock_qty_minus_price", (F.col("stock_qty") - F.col("price")))
)

products_stats = (
    filtered_products
    .groupBy("brand")
    .agg(
        F.sort_array(F.collect_set("brand")).alias("brand_list"),
        F.count("price").alias("price_count"),
    )
)

result = (
    products_stats
    .filter(F.col("price_count") >= 6)
    .select("brand", "price_count")
    .orderBy(F.col("price_count").desc())
)
