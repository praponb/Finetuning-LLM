from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("amount") < 460)
    .groupBy(
        F.year("order_date").alias("order_year"),
        F.date_trunc("MONTH", "order_date").alias("order_month"),
    )
    .agg(
        F.sum(F.when(F.col("channel") == "partner", F.col("amount")).otherwise(0)).alias("partner_amount"),
        F.count("amount").alias("amount_count"),
    )
)
