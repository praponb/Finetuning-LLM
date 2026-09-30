from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("units") <= 36)
    .select(
        "id",
        "sale_date",
        "units",
        "revenue",
        F.col("units").cast("string").alias("units_str"),
    )
)
