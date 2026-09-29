from pyspark.sql import functions as F

result = (
    df_store_sales
    .select(
        "id",
        "product_id",
        F.date_format("sale_date", "yyyy-MM-dd").alias("sale_date_label"),
        F.least("units", "revenue").alias("least_units_revenue"),
    )
)
