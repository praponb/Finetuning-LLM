from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "left")
    .filter((F.col("ss.units") == 15) & (F.col("ss.sale_date") > "2023-11-01"))
    .select(
        F.col("ss.id"),
        F.col("ss.sale_date"),
        F.col("p.category"),
        F.col("p.supplier_id"),
        F.col("p.launch_date"),
    )
)
