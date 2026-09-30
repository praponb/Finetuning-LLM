from pyspark.sql import functions as F

result = (
    df_store_sales
    .select(
        "store_id",
        "revenue",
        F.datediff(F.current_date(), "sale_date").alias("days_since_sale_date"),
        F.round((F.col("revenue") * 0.9), 2).alias("adjusted_revenue"),
    )
)
