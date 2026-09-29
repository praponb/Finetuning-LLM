from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("units").between(29, 39))
    .select(
        "revenue",
        F.round((F.col("revenue") / F.col("units")), 2).alias("revenue_per_unit"),
        F.datediff(F.current_date(), "sale_date").alias("days_since_sale_date"),
    )
)
