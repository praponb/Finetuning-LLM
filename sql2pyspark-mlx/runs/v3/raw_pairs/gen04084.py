from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter((F.col("revenue") > 2100) & F.month("sale_date").isin([6, 8, 10]))
    .select(
        "units",
        "store_id",
        F.datediff(F.current_date(), "sale_date").alias("days_since_sale_date"),
    )
)
