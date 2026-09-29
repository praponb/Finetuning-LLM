from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_store_sales.alias("ss"), F.col("ss.product_id") == F.col("p.id"), "left")
    .filter(F.col("ss.store_id").isin([1, 2, 3, 5]))
    .select(
        F.col("p.launch_date"),
        F.col("p.product_name"),
        F.col("p.cost"),
        F.col("ss.product_id"),
        F.col("ss.units"),
        F.col("ss.sale_date"),
        F.coalesce(F.col("p.product_name"), F.lit("Unknown")).alias("product_name_filled"),
    )
    .orderBy(F.col("units").desc())
)
