from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.filter(~F.col("category").isin(["Electronics", "Fashion"])).alias("p"), F.col("ss.product_id") == F.col("p.id"), "left_semi")
    .select(
        F.col("ss.id"),
        F.col("ss.product_id"),
        F.col("ss.store_id"),
        F.col("ss.sale_date"),
    )
)
