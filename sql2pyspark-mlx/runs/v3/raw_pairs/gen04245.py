from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "inner")
    .filter((F.col("p.stock_qty") == 333) & (F.col("p.price") < 740))
    .select(
        F.col("ss.id"),
        F.col("ss.sale_date"),
        F.col("ss.product_id"),
        F.col("p.category"),
        F.col("p.launch_date"),
        F.col("p.brand"),
    )
    .orderBy(F.col("id").desc())
)
