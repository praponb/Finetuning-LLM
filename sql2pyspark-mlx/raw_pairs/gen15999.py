from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "left")
    .join(df_stores.alias("s"), F.col("ss.store_id") == F.col("s.id"), "left")
    .select(
        F.col("ss.sale_date"),
        F.col("ss.units"),
        F.col("p.cost"),
        F.col("p.product_name"),
        F.col("s.sqft"),
    )
    .orderBy(F.col("product_name").desc())
)
