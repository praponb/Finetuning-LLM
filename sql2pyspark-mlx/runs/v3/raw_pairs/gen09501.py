from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "right")
    .select(F.col("ss.sale_date"), F.col("p.stock_qty"), F.col("p.cost"))
    .orderBy(F.col("stock_qty").desc())
)
