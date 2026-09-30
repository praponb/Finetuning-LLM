from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("sale_date").between("2023-12-27", "2024-01-01"))
    .select(
        "id",
        "revenue",
        "sale_date",
        F.last_day("sale_date").alias("sale_date_month_end"),
    )
    .orderBy("sale_date_month_end", "id")
    .limit(20)
)
