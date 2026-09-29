from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter((F.col("revenue").between(3400, 3900) & (F.col("sale_date") > "2024-06-01")) | F.col("revenue").between(1300, 1800))
    .select(
        "sale_date",
        "store_id",
        F.date_trunc("MONTH", "sale_date").alias("sale_date_period"),
    )
)
