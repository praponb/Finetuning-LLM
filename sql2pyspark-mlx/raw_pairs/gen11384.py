from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("store_id").isin([2, 3, 4]) & (F.col("store_id").isin([2, 3]) | F.col("units").between(17, 32)))
    .select(
        "id",
        "sale_date",
        "product_id",
        "revenue",
        F.col("units").cast("double").alias("units_str"),
        F.round((F.col("revenue") / F.col("units")), 2).alias("revenue_per_unit"),
    )
)
