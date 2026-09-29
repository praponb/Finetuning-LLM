from pyspark.sql import functions as F

valid_store_sales = (
    df_store_sales
    .filter((F.col("units") > 27) & F.col("store_id").isin([1, 2, 4, 5]))
    .withColumn("days_since_sale_date", F.datediff(F.current_date(), "sale_date"))
)

agg = (
    valid_store_sales
    .groupBy("store_id")
    .agg(
        F.round(F.stddev("days_since_sale_date"), 2).alias("stddev_days_since_sale_date"),
        F.round(F.avg("days_since_sale_date"), 2).alias("avg_days_since_sale_date"),
    )
)

result = agg.select("store_id", "avg_days_since_sale_date")
