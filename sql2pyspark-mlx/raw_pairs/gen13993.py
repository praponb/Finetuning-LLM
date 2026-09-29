from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "left")
    .filter(F.col("p.brand") == "Stark")
    .select(
        F.col("ss.id"),
        F.col("ss.revenue"),
        F.col("ss.store_id"),
        F.col("p.cost"),
        F.col("p.product_name"),
    )
)
