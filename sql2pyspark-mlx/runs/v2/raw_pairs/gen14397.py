from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter((F.col("product_id") == 3) & F.col("units").between(28, 29) & (F.col("sale_date") <= "2025-09-07"))
    .groupBy("product_id")
    .agg(
        F.max("units").alias("max_units"),
        (F.max("revenue") - F.min("revenue")).alias("revenue_range"),
    )
    .orderBy(F.col("revenue_range").desc())
)
