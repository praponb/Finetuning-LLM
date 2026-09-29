from pyspark.sql import functions as F

result = (
    df_store_sales
    .select(
        "sale_date",
        "store_id",
        F.datediff(F.current_date(), "sale_date").alias("days_since_sale_date"),
        F.col("units").cast("double").alias("units_str"),
    )
)
