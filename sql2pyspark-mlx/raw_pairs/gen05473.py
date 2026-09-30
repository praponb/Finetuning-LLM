from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("email").isNotNull())
    .select(F.col("hire_date").alias("event_date"))
    .unionByName(
        df_orders
        .filter(F.col("discount") >= 0.14)
        .select(F.col("order_date").alias("event_date"))
    )
    .orderBy("event_date")
)
