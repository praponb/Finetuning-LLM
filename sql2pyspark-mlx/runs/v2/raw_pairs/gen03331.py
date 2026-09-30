from pyspark.sql import functions as F

result = (
    df_products
    .select(
        "stock_qty",
        F.date_trunc("MONTH", "launch_date").alias("launch_date_period"),
    )
)
