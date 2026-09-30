from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("store_id").isin([2, 6]))
    .select(
        "id",
        "store_id",
        "units",
        "revenue",
        F.least("units", "revenue").alias("least_units_revenue"),
    )
)
