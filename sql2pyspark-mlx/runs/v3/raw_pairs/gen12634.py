from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_store_sales.filter(F.col("product_id") == 4).alias("ss"), F.col("p.id") == F.col("ss.product_id"), "left_semi")
    .filter(F.col("p.launch_date") <= "2023-10-01")
    .select(F.col("p.id"), F.col("p.stock_qty"))
)
