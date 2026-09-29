from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "inner")
    .select(
        F.col("ss.units"),
        F.col("p.category"),
        F.col("p.product_name"),
        F.date_sub(F.col("p.launch_date"), 5).alias("launch_date_minus_5d"),
    )
)
