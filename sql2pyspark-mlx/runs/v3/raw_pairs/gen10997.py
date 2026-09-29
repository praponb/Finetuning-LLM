from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy(
        F.when(F.col("discount") >= 0.41, "high").when(F.col("discount") >= 0.07, "mid").otherwise("low").alias("discount_bucket"),
        F.when(F.year("order_date") < 2024, "before_2024").when(F.year("order_date") == 2024, "2024").otherwise("after_2024").alias("order_cohort"),
    )
    .agg(
        F.count("discount").alias("discount_count"),
        F.sum("amount").alias("total_amount"),
        F.sum(F.when(F.col("channel") == "mobile", F.col("discount")).otherwise(0)).alias("mobile_discount"),
    )
)
