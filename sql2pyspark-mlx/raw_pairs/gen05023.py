from pyspark.sql import functions as F

result = (
    df_products
    .groupBy(
        F.date_trunc("MONTH", "launch_date").alias("activity_month"),
        F.date_trunc("YEAR", "launch_date").alias("year_start"),
    )
    .agg(
        F.count("*").alias("row_count"),
        F.bit_or("stock_qty").alias("stock_qty_bit_or"),
        F.round(F.avg("stock_qty"), 2).alias("avg_stock_qty"),
    )
)
