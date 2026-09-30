from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "right")
    .join(df_stores.alias("s"), F.col("s.id") == F.col("ss.store_id"), "left")
    .select(
        F.col("ss.id"),
        F.col("ss.sale_date"),
        F.col("p.cost"),
        F.col("p.category"),
        F.col("s.city"),
    )
    .orderBy(F.col("id").desc(), "city")
)
