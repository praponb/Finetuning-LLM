from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy(
        F.when(F.year("order_date") < 2024, "before_2024").when(F.year("order_date") == 2024, "2024").otherwise("after_2024").alias("order_cohort"),
    )
    .agg(F.count(F.when(F.col("status") == "CANCELLED", 1)).alias("cancelled_count"))
    .orderBy("order_cohort")
    .limit(20)
)
