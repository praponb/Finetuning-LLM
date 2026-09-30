from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_store_sales.alias("ss"), F.col("p.id") == F.col("ss.product_id"), "left")
    .filter(F.col("p.product_name").like("C%"))
    .select(
        F.col("p.launch_date"),
        F.col("p.category"),
        F.col("ss.sale_date"),
        F.col("ss.store_id"),
    )
)
