from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("sale_date") < "2024-04-01")
    .select(
        "id",
        "store_id",
        "revenue",
        F.greatest("units", "revenue").alias("greatest_units_revenue"),
    )
)
