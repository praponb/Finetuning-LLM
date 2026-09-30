from pyspark.sql import functions as F

result = (
    df_store_sales
    .select(
        "id",
        "sale_date",
        "store_id",
        "product_id",
        F.weekofyear("sale_date").alias("sale_date_weekofyear"),
    )
    .orderBy("sale_date")
)
