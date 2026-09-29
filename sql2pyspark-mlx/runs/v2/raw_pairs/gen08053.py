from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "outer")
    .select(
        F.col("ss.id"),
        F.col("ss.product_id"),
        F.col("p.supplier_id"),
        F.col("p.price"),
        F.col("p.category"),
    )
    .orderBy(F.col("id").desc(), "category")
)
