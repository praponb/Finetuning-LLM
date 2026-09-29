from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("store_id").isin([1, 2, 3]) & (F.col("store_id") == 3))
    .select(
        "id",
        "store_id",
        "units",
        F.last_day("sale_date").alias("sale_date_month_end"),
    )
    .orderBy(F.col("id").desc())
)
