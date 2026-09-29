from pyspark.sql import functions as F

result = (
    df_store_sales
    .select(
        "id",
        "product_id",
        "units",
        F.round((F.col("units") / F.col("revenue")), 2).alias("revenue_per_unit"),
    )
    .orderBy(F.col("id").desc())
)
