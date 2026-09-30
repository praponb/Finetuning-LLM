from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("revenue").isNull())
    .select(
        "id",
        "store_id",
        "units",
        F.date_sub("sale_date", 19).alias("sale_date_minus_19d"),
        F.datediff(F.current_date(), "sale_date").alias("days_since_sale_date"),
    )
)
