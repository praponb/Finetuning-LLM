from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_store_sales.alias("ss"), F.col("ss.product_id") == F.col("p.id"), "left")
    .filter(F.col("ss.units") >= 14)
    .groupBy(F.col("p.id"), F.col("p.category"))
    .agg(
        F.count(F.col("ss.id")).alias("store_sale_count"),
        F.count(F.col("ss.units")).alias("units_count"),
    )
    .filter(F.col("store_sale_count") > 4)
    .orderBy("category")
)
