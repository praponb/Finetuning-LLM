from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("status") == "RETURNED")
    .groupBy(
        F.when(F.year("order_date") < 2024, "before_2024").when(F.year("order_date") == 2024, "2024").otherwise("after_2024").alias("order_cohort"),
    )
    .agg(F.round(F.stddev("shipping_fee"), 2).alias("stddev_shipping_fee"))
    .filter(F.col("stddev_shipping_fee") >= 77)
    .select("order_cohort", "stddev_shipping_fee")
    .orderBy(F.col("order_cohort").desc())
)
