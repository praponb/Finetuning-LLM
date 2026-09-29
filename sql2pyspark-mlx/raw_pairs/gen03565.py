from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "outer")
    .filter(F.col("ss.product_id").isin([1, 2, 7, 10]) & F.col("p.supplier_id").isNotNull())
    .select(
        F.col("ss.id"),
        F.col("ss.units"),
        F.col("ss.store_id"),
        F.col("p.supplier_id"),
        F.col("p.launch_date"),
    )
)
