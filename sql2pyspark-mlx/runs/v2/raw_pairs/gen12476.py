from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_store_sales.alias("ss"), F.col("ss.product_id") == F.col("p.id"), "left")
    .filter(F.col("ss.sale_date") < "2024-05-01")
    .groupBy(F.col("p.category"))
    .agg(
        F.count(F.col("ss.id")).alias("store_sale_count"),
        F.countDistinct(F.col("ss.units")).alias("unique_stores"),
        F.sum(F.col("ss.units")).alias("total_units"),
    )
)
