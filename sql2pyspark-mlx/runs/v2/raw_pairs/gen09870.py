from pyspark.sql import functions as F

result = (
    df_store_sales
    .select(
        "product_id",
        "sale_date",
        F.date_sub("sale_date", 56).alias("sale_date_minus_56d"),
    )
)
