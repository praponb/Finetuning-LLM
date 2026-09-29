from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("customer_id").isNull())
    .groupBy(
        F.when(F.year("order_date") < 2024, "before_2024").when(F.year("order_date") == 2024, "2024").otherwise("after_2024").alias("order_cohort"),
        "customer_id",
    )
    .agg(
        F.count("*").alias("num_orders"),
        F.min("amount").alias("min_amount"),
    )
    .orderBy(F.col("min_amount").desc(), "order_cohort", "customer_id")
    .limit(5)
)
