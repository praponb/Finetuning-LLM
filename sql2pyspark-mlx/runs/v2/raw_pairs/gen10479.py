from pyspark.sql import functions as F

result = (
    df_products
    .select(
        "stock_qty",
        "brand",
        F.trim("brand").alias("clean_brand"),
        F.datediff(F.current_date(), "launch_date").alias("days_since_launch_date"),
    )
    .orderBy("days_since_launch_date", "brand")
)
