from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("units") != 35)
    .select(
        "id",
        "sale_date",
        "product_id",
        "revenue",
        F.datediff(F.current_date(), "sale_date").alias("days_since_sale_date"),
    )
)
