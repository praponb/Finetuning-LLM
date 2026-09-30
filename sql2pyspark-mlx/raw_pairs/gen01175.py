from pyspark.sql import functions as F

result = (
    df_products
    .alias("s")
    .join(df_store_sales.alias("c"), (F.col("c.product_id") == F.col("s.id")) & (F.col("c.revenue") >= 3500), "left_semi")
)
