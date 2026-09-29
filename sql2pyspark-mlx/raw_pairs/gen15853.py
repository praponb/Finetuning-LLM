from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_stores.filter(F.col("sqft") >= 5387).alias("s"), F.col("ss.store_id") == F.col("s.id"), "left_semi")
    .filter(F.col("ss.revenue").isNotNull())
    .select(F.col("ss.id"), F.col("ss.sale_date"), F.col("ss.product_id"))
    .orderBy(F.col("product_id").desc())
)
