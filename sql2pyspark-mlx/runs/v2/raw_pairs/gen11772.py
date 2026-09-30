from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_stores.alias("s"), F.col("s.id") == F.col("ss.store_id"), "outer")
    .select(
        F.col("ss.sale_date"),
        F.col("ss.store_id"),
        F.col("ss.product_id"),
        F.col("s.opened_date"),
    )
    .orderBy("store_id", F.col("product_id").desc())
)
