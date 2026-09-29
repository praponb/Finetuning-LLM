from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "inner")
    .filter(F.col("ss.product_id").isin([8, 9, 10]))
    .select(
        F.col("ss.sale_date"),
        F.col("p.cost"),
        F.col("p.brand"),
        F.weekofyear(F.col("p.launch_date")).alias("launch_date_weekofyear"),
    )
)
