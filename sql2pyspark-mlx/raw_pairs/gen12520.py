from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.year("order_date") >= 2024)
    .groupBy(
        "status",
        F.when(F.year("order_date") < 2024, "before_2024").when(F.year("order_date") == 2024, "2024").otherwise("after_2024").alias("order_cohort"),
    )
    .agg(F.sum("shipping_fee").alias("total_shipping_fee"))
    .orderBy("total_shipping_fee", "status", "order_cohort")
    .limit(50)
)
