from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.filter(F.month("launch_date").isin([3, 9, 12])).alias("p"), F.col("ss.product_id") == F.col("p.id"), "left_anti")
    .filter(F.col("ss.units") == 32)
    .select(F.col("ss.id"), F.col("ss.sale_date"), F.col("ss.product_id"))
    .orderBy(F.col("product_id").desc(), F.col("id").desc())
)
