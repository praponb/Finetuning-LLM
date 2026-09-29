from pyspark.sql import functions as F

result = (
    df_orders
    .filter((F.col("customer_id") == 17) & (F.col("store_id") == 19))
    .groupBy(
        F.when(F.year("order_date") < 2024, "before_2024").when(F.year("order_date") == 2024, "2024").otherwise("after_2024").alias("order_cohort"),
        "status",
    )
    .agg(
        (F.max("discount") - F.min("discount")).alias("discount_range"),
        F.max("order_date").alias("latest_order_date"),
        F.min("amount").alias("min_amount"),
    )
)
