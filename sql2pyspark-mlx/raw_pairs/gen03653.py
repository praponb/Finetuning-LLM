from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.filter(F.lower("product_name").like("%esk%")).alias("p"), F.col("ss.product_id") == F.col("p.id"), "left_anti")
    .select(F.col("ss.id"), F.col("ss.units"), F.col("ss.sale_date"))
)
