from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.filter(F.col("cost") < 560).alias("p"), F.col("ss.product_id") == F.col("p.id"), "left_anti")
    .filter(F.col("ss.product_id") == 7)
    .select(
        F.col("ss.id"),
        F.col("ss.product_id"),
        F.col("ss.units"),
        F.col("ss.revenue"),
    )
)
