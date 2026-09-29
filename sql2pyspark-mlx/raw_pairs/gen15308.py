from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_store_sales.alias("ss"), F.col("p.id") == F.col("ss.product_id"), "inner")
    .filter(F.col("p.cost") < 330)
    .select(
        F.col("p.brand"),
        F.col("p.cost"),
        F.col("ss.store_id"),
        F.col("ss.product_id"),
        F.col("ss.revenue"),
    )
)
