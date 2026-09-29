from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("p.id") == F.col("ss.product_id"), "left")
    .filter(F.col("p.category").isin(["Electronics", "Grocery"]))
    .select(
        F.col("ss.id"),
        F.col("ss.store_id"),
        F.col("ss.sale_date"),
        F.col("ss.product_id"),
        F.col("p.category"),
        F.col("p.brand"),
        F.col("p.product_name"),
    )
)
