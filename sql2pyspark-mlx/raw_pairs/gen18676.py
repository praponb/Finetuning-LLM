from pyspark.sql import functions as F

result = (
    df_orders
    .filter((F.col("shipping_fee") <= 39) & (F.col("order_date") >= "2025-05-01"))
    .groupBy(
        F.when(F.year("order_date") < 2024, "before_2024").when(F.year("order_date") == 2024, "2024").otherwise("after_2024").alias("order_cohort"),
    )
    .agg(F.count("*").alias("total_count"))
)
