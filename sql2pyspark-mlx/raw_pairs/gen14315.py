from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_store_sales.alias("ss"), F.col("p.id") == F.col("ss.product_id"), "left")
    .filter(F.col("ss.units") <= 30)
    .select(
        F.col("p.product_name"),
        F.col("ss.revenue"),
        F.col("ss.sale_date"),
        F.col("ss.units"),
    )
    .orderBy("units", "product_name")
)
