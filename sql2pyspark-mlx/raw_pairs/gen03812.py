from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.filter(F.size("tags") >= 1).alias("p"), F.col("ss.product_id") == F.col("p.id"), "left_anti")
    .filter(F.col("ss.sale_date") > "2023-04-06")
    .select(F.col("ss.id"), F.col("ss.product_id"), F.col("ss.store_id"))
    .orderBy(F.col("product_id").desc(), "store_id")
)
