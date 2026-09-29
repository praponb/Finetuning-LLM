from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "outer")
    .filter(F.col("ss.units").between(20, 36))
    .select(
        F.col("ss.id"),
        F.col("ss.store_id"),
        F.col("p.product_name"),
        F.col("p.launch_date"),
        F.col("p.brand"),
    )
)
