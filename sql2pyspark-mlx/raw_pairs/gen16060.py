from pyspark.sql import functions as F

result = (
    df_store_sales
    .select(
        "revenue",
        "units",
        "sale_date",
        F.coalesce("revenue", F.lit(0)).alias("revenue_filled"),
        F.col("units").cast("string").alias("units_num"),
    )
    .orderBy("units")
)
