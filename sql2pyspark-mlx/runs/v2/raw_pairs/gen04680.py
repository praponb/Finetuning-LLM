from pyspark.sql import functions as F

result = (
    df_store_sales.alias("store_sales")
    .join(df_products.filter(F.col("category") == "Sports").alias("products"), F.col("store_sales.product_id") == F.col("products.id"), "left_semi")
    .select(
        F.col("store_sales.id"),
        F.col("store_sales.units"),
        F.col("store_sales.sale_date"),
    )
    .orderBy(F.col("id").desc(), F.col("units").desc())
)
