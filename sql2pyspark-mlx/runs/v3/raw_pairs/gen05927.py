from pyspark.sql import functions as F

filtered_orders = (
    df_orders
    .filter(F.col("customer_id").isin([15, 16, 19]) & F.col("discount").between(0.23, 0.26) & (F.col("discount") > 0.18))
    .withColumn("clean_status", F.trim("status"))
)

agg = (
    filtered_orders
    .groupBy(
        F.when(F.year("order_date") < 2024, "before_2024").when(F.year("order_date") == 2024, "2024").otherwise("after_2024").alias("order_cohort"),
    )
    .agg(F.max("order_date").alias("latest_order_date"))
)

result = (
    agg
    .select("order_cohort", "latest_order_date")
    .orderBy(F.col("order_cohort").desc())
)
