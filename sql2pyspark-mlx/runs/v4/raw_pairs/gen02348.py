from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("product_id").isin([1, 3, 8, 10]) & (F.col("revenue") > 1700))
    .select(
        "id",
        "store_id",
        "revenue",
        "units",
        F.datediff(F.current_date(), "sale_date").alias("days_since_sale_date"),
    )
)
