from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("store_id").isin([3, 4, 5, 6]) & ((F.col("revenue") < 1400) | F.col("product_id").isin([6, 10])))
    .select(
        "id",
        "product_id",
        F.round((F.col("units") / F.col("revenue")), 2).alias("revenue_per_unit"),
    )
    .orderBy("id")
)
