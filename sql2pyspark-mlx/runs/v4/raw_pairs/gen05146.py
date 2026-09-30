from pyspark.sql import functions as F

result = (
    df_store_sales
    .select(
        "id",
        "product_id",
        "units",
        "store_id",
        F.add_months("sale_date", 12).alias("sale_date_plus_months"),
    )
)
