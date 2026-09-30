from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("discount") < 0.21)
    .groupBy(
        "channel",
        "customer_id",
        F.when(F.year("order_date") < 2024, "before_2024").when(F.year("order_date") == 2024, "2024").otherwise("after_2024").alias("order_cohort"),
    )
    .agg(F.max("discount").alias("max_discount"))
    .filter(F.col("max_discount") >= 0.38)
    .orderBy("customer_id")
)
