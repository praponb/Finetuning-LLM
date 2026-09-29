from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("supplier_id").isin([3, 5, 13, 32]) & (F.col("launch_date") <= "2025-07-01"))
    .select(
        "id",
        "brand",
        "launch_date",
        F.last_day("launch_date").alias("launch_date_month_end"),
    )
    .orderBy("launch_date_month_end", "id")
    .limit(10)
)
