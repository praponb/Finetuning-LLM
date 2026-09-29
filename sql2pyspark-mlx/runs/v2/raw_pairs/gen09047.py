from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_store_sales.alias("ss"), F.col("ss.product_id") == F.col("p.id"), "left")
    .filter(F.col("ss.product_id").isin([1, 9]))
    .select(
        F.col("p.price"),
        F.col("ss.sale_date"),
        F.when(F.col("ss.revenue") >= 4300, "High").when(F.col("ss.revenue") >= 500, "Medium").otherwise("Low").alias("revenue_tier"),
    )
    .orderBy(F.col("revenue_tier").desc())
)
