from pyspark.sql import functions as F

result = (
    df_store_sales
    .select(
        "id",
        "sale_date",
        "store_id",
        F.date_trunc("MONTH", "sale_date").alias("sale_date_period"),
    )
)
