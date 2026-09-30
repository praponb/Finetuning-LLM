from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_store_sales.alias("ss"), F.col("ss.product_id") == F.col("p.id"), "left")
    .filter(F.col("ss.store_id").isin([2, 4, 5]))
    .select(
        F.col("p.launch_date"),
        F.col("p.price"),
        F.col("p.category"),
        F.col("ss.product_id"),
        F.col("ss.units"),
        F.col("ss.sale_date"),
    )
    .orderBy(F.col("launch_date").desc())
)
