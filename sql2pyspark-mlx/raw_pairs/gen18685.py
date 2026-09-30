from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "left")
    .join(df_stores.alias("s"), F.col("ss.store_id") == F.col("s.id"), "left")
    .select(
        F.col("ss.id"),
        F.col("ss.product_id"),
        F.col("ss.store_id"),
        F.col("p.launch_date"),
        F.col("s.region"),
        F.col("s.store_name"),
        F.concat_ws("-", F.col("p.brand"), F.col("p.category")).alias("brand_category_key"),
    )
)
