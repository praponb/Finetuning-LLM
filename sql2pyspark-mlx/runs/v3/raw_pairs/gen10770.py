from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("p.id") == F.col("ss.product_id"), "inner")
    .join(df_stores.alias("s"), F.col("s.id") == F.col("ss.store_id"), "inner")
    .filter(~F.col("p.brand").isin(["Initech", "Umbrella", "Stark"]) | F.col("p.product_name").like("D%"))
    .select(
        F.col("ss.id"),
        F.col("ss.revenue"),
        F.col("p.brand"),
        F.col("s.store_name"),
    )
)
