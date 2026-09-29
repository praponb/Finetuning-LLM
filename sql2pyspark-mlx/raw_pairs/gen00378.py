from pyspark.sql import functions as F

result = (
    df_store_sales.alias("store_sales")
    .join(df_stores.alias("stores"), F.col("store_sales.store_id") == F.col("stores.id"), "left")
    .filter((F.col("stores.sqft") < 6614) & F.col("stores.store_name").like("Ol%"))
    .select(
        F.col("store_sales.sale_date"),
        F.col("store_sales.revenue"),
        F.col("stores.region"),
        F.col("stores.store_name"),
        F.col("stores.city"),
        F.when(F.col("store_sales.revenue") >= 4100, "High").when(F.col("store_sales.revenue") >= 1800, "Medium").otherwise("Low").alias("revenue_tier"),
    )
    .orderBy(F.col("revenue").desc(), "region")
)
