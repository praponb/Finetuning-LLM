from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_store_sales.alias("ss"), F.col("p.id") == F.col("ss.product_id"), "left")
    .groupBy(F.col("p.brand"))
    .agg(
        F.count(F.col("ss.id")).alias("store_sale_count"),
        F.min(F.col("ss.sale_date")).alias("first_sale_date"),
    )
)
