from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("order_date") >= "2024-04-01")
    .groupBy(
        F.date_trunc("MONTH", "order_date").alias("order_month"),
        F.when(F.year("order_date") < 2024, "before_2024").when(F.year("order_date") == 2024, "2024").otherwise("after_2024").alias("order_cohort"),
    )
    .agg(
        F.round(F.avg("discount"), 2).alias("avg_discount"),
        F.min("shipping_fee").alias("min_shipping_fee"),
    )
    .filter(F.col("avg_discount") > 60)
    .orderBy(F.col("avg_discount").desc(), "order_month", "order_cohort")
    .limit(5)
)
