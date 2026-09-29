from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.filter(F.col("price") <= 500).alias("p"), F.col("ss.product_id") == F.col("p.id"), "left_semi")
    .filter(F.col("ss.product_id").isin([2, 3, 4]))
    .select(
        F.col("ss.id"),
        F.col("ss.revenue"),
        F.col("ss.sale_date"),
        F.col("ss.units"),
    )
)
