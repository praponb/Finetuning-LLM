from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("status").isin(["CANCELLED", "RETURNED"]))
    .groupBy(
        "status",
        F.when(F.year("order_date") < 2024, "before_2024").when(F.year("order_date") == 2024, "2024").otherwise("after_2024").alias("order_cohort"),
    )
    .agg(F.count("amount").alias("amount_count"))
    .orderBy(F.col("order_cohort").desc(), F.col("amount_count").desc())
)
