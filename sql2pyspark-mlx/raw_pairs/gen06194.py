from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy(F.month("order_date").alias("order_month"), "channel")
    .agg(
        F.count("*").alias("cnt"),
        F.count(F.when(F.col("channel") == "partner", 1)).alias("partner_count"),
    )
    .filter(F.col("partner_count") >= 3)
    .orderBy("partner_count", F.col("cnt").desc(), "order_month", "channel")
    .limit(1)
)
