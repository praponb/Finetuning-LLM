from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("customer_id").isNotNull())
    .groupBy(
        "status",
        F.when(F.year("order_date") < 2024, "before_2024").when(F.year("order_date") == 2024, "2024").otherwise("after_2024").alias("order_cohort"),
    )
    .agg(F.count("*").alias("total_count"))
    .filter(F.col("total_count") > 5)
    .orderBy("order_cohort", "status")
    .limit(50)
)
