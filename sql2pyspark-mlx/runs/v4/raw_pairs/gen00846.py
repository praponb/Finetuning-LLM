from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "outer")
    .join(df_stores.alias("s"), F.col("ss.store_id") == F.col("s.id"), "left")
    .filter(F.col("ss.store_id").isin([1, 2, 5]))
    .groupBy(
        F.when(F.col("p.price") >= 910, "high").when(F.col("p.price") >= 270, "mid").otherwise("low").alias("price_bucket"),
        F.month(F.col("p.launch_date")).alias("launch_month"),
    )
    .agg(F.min(F.col("ss.sale_date")).alias("first_sale_date"))
    .orderBy("price_bucket")
)
