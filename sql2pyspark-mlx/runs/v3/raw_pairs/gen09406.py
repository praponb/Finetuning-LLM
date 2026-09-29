from pyspark.sql import functions as F

result = (
    df_store_sales
    .select(
        "id",
        "product_id",
        "revenue",
        F.date_sub("sale_date", 10).alias("sale_date_minus_10d"),
        F.date_format("sale_date", "MMM yyyy").alias("sale_date_label"),
    )
)
