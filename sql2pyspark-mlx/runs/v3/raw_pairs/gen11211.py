from pyspark.sql import functions as F

result = (
    df_store_sales
    .select(
        "product_id",
        "sale_date",
        "units",
        F.last_day("sale_date").alias("sale_date_month_end"),
        F.round((F.col("units") / F.col("revenue")), 2).alias("revenue_per_unit"),
    )
    .orderBy(F.col("units").desc())
)
