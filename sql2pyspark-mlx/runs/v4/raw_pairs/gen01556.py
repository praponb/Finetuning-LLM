from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_store_sales.filter(F.col("store_id") == 2).alias("ss"), F.col("p.id") == F.col("ss.product_id"), "left_semi")
    .select(
        F.col("p.id"),
        F.col("p.product_name"),
        F.col("p.launch_date"),
        F.col("p.cost"),
    )
)
