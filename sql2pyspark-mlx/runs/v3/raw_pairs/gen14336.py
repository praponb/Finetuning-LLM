from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("units") < 14)
    .select(
        "revenue",
        "product_id",
        F.greatest("units", "revenue").alias("greatest_units_revenue"),
        F.round((F.col("revenue") / F.col("units")), 2).alias("revenue_per_unit"),
    )
)
