from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("units").between(13, 40))
    .select(
        "id",
        "revenue",
        "product_id",
        "store_id",
        F.datediff(F.current_date(), "sale_date").alias("days_since_sale_date"),
    )
)
