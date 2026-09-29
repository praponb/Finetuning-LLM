from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("units").between(12, 28) | F.col("product_id").isin([2, 3, 5, 10]))
    .select(
        "store_id",
        F.round((F.col("revenue") / F.col("units")), 2).alias("revenue_per_unit"),
    )
)
