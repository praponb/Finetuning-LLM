from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy(F.date_format("order_date", "yyyy-MM").alias("order_month"))
    .agg(
        F.sum(F.when(F.col("channel") == "partner", 1).otherwise(0)).alias("partner_count"),
    )
    .filter(F.col("partner_count") >= 74)
)
