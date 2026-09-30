from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("revenue").isNotNull())
    .select(
        "sale_date",
        "revenue",
        F.date_trunc("MONTH", "sale_date").alias("sale_date_period"),
    )
)
