from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy(F.year("order_date").alias("order_year"))
    .agg(
        F.count(F.when(F.col("channel") == "partner", 1)).alias("partner_count"),
        F.avg("amount").alias("avg_amount"),
    )
    .orderBy(F.col("order_year").desc())
)
