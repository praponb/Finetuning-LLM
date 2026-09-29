from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("sale_date").between("2024-09-22", "2025-02-01"))
    .select(
        "id",
        "units",
        F.dayofweek("sale_date").alias("sale_date_dayofweek"),
        F.last_day("sale_date").alias("sale_date_month_end"),
    )
    .orderBy(F.col("id").desc())
    .limit(10)
)
