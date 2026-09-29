from pyspark.sql import functions as F

result = (
    df_store_sales
    .select(
        "units",
        "revenue",
        "sale_date",
        F.col("units").cast("string").alias("units_str"),
        F.round((F.col("revenue") / F.col("units")), 2).alias("revenue_per_unit"),
    )
    .orderBy("sale_date")
)
