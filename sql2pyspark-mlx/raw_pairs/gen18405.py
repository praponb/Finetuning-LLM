from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_stores.alias("s"), F.col("ss.store_id") == F.col("s.id"), "inner")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "inner")
    .filter(F.col("s.region").isin(["South", "West", "North"]))
    .select(
        F.col("ss.units"),
        F.col("s.region"),
        F.col("p.product_name"),
        F.col("p.launch_date"),
    )
    .orderBy("product_name", F.col("units").desc())
)
