from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_store_sales.alias("ss"), F.col("ss.product_id") == F.col("p.id"), "left")
    .filter(F.col("p.price").isNull())
    .groupBy(F.col("p.id"), F.col("p.brand"))
    .agg(
        F.count(F.col("ss.id")).alias("store_sale_count"),
        F.max(F.col("ss.sale_date")).alias("last_sale_date"),
    )
    .orderBy("last_sale_date", "id")
)
