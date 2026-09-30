from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("sale_date") <= "2023-04-01")
    .select(
        "revenue",
        "sale_date",
        "units",
        (F.col("units") * F.col("revenue")).alias("revenue_per_unit"),
        F.dayofmonth("sale_date").alias("sale_date_day"),
    )
)
