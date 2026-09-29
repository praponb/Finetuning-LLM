from pyspark.sql import functions as F

result = (
    df_store_sales
    .select(
        "id",
        "revenue",
        "store_id",
        "units",
        F.least("revenue", "units").alias("least_revenue_units"),
        (F.col("units") * F.col("revenue")).alias("revenue_per_unit"),
    )
)
