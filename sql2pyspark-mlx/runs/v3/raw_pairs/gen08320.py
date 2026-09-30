from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_store_sales.alias("ss"), F.col("p.id") == F.col("ss.product_id"), "inner")
    .filter(F.col("ss.revenue") >= 2400)
    .select(
        F.col("p.price"),
        F.col("p.brand"),
        F.col("p.launch_date"),
        F.col("ss.revenue"),
    )
    .orderBy(F.col("revenue").desc(), "launch_date")
)
