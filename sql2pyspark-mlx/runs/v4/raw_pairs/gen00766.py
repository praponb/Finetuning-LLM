from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy(F.date_trunc("week", "order_date").alias("week_start"), "channel")
    .agg(
        F.corr("amount", "discount").alias("corr_amount_discount"),
        F.sum("amount").alias("total_amount"),
    )
    .filter(F.col("corr_amount_discount") < 0)
    .orderBy("week_start")
)
