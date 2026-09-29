from pyspark.sql import functions as F

result = (
    df_products
    .filter((F.col("brand") == "Initech") & F.col("supplier_id").isNotNull())
    .groupBy("brand", F.date_format("launch_date", "yyyy-MM").alias("launch_month"))
    .agg(
        F.count("price").alias("price_count"),
        F.round(F.avg("price"), 2).alias("avg_price"),
    )
    .orderBy("price_count")
)
