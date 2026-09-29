from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("stock_qty") < 136)
    .groupBy(F.date_trunc("MONTH", "launch_date").alias("launch_month"))
    .agg(
        F.sum(F.when(F.col("category") == "Fashion", F.col("stock_qty")).otherwise(0)).alias("fashion_stock_qty"),
        F.avg("cost").alias("avg_cost"),
    )
    .filter(F.col("avg_cost") < 430)
    .drop("avg_cost")
)
