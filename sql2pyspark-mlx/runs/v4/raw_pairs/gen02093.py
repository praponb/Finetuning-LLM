from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy(
        F.when(F.year("order_date") < 2024, "before_2024").when(F.year("order_date") == 2024, "2024").otherwise("after_2024").alias("order_cohort"),
    )
    .agg(
        F.sum(F.when(F.col("status") == "CANCELLED", F.col("amount")).otherwise(0)).alias("cancelled_amount"),
        F.min("order_date").alias("min_order_date"),
    )
    .filter(F.col("cancelled_amount") >= 67)
    .select("order_cohort", "cancelled_amount", "min_order_date")
    .orderBy(F.col("cancelled_amount").desc(), "order_cohort")
)
