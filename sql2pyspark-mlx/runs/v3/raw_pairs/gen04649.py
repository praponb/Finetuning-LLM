from pyspark.sql import functions as F

result = (
    df_store_sales
    .select(
        "id",
        "units",
        "product_id",
        F.datediff(F.current_date(), "sale_date").alias("days_since_sale_date"),
    )
)
