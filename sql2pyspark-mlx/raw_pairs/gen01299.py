from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "inner")
    .filter(F.col("ss.sale_date") >= "2023-04-01")
    .select(
        F.col("ss.revenue"),
        F.col("ss.store_id"),
        F.col("ss.sale_date"),
        F.col("p.price"),
        F.col("p.product_name"),
    )
)
