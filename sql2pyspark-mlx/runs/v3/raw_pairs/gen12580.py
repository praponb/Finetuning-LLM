from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "inner")
    .groupBy(
        F.col("p.supplier_id"),
        F.when(F.col("p.brand").isin(["Umbrella", "Globex"]), "tier_1").otherwise("other").alias("brand_group"),
    )
    .agg(F.round(F.stddev(F.col("ss.revenue")), 2).alias("stddev_revenue"))
)
