from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_stores.alias("s"), F.col("ss.store_id") == F.col("s.id"), "inner")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "left")
    .filter(F.col("p.supplier_id").isNull())
    .groupBy(
        F.when(F.col("s.sqft") >= 12299, "high").when(F.col("s.sqft") >= 3093, "mid").otherwise("low").alias("sqft_bucket"),
    )
    .agg(F.max(F.col("ss.units")).alias("max_units"))
    .orderBy("sqft_bucket")
)
