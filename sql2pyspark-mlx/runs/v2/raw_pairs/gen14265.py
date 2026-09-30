from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_store_sales.filter(F.col("store_id").isin([4, 6])).alias("ss"), F.col("p.id") == F.col("ss.product_id"), "left_semi")
    .filter(~F.col("p.category").isin(["Grocery", "Fashion"]))
    .select(
        F.col("p.id"),
        F.col("p.product_name"),
        F.col("p.category"),
        F.col("p.brand"),
    )
    .orderBy("product_name")
)
