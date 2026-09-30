from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_stores.alias("s"), F.col("ss.store_id") == F.col("s.id"), "inner")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "left")
    .groupBy(
        F.col("p.brand"),
        F.quarter(F.col("p.launch_date")).alias("launch_quarter"),
    )
    .agg(F.count("*").alias("store_sale_count"))
    .orderBy(F.col("store_sale_count").desc(), "brand", "launch_quarter")
    .limit(20)
)
