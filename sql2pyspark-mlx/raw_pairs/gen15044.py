from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("order_date").isNotNull())
    .select("order_date")
    .unionByName(
        df_employees
        .filter(F.col("hire_date").isNotNull())
        .select(F.col("hire_date").alias("order_date"))
    )
    .distinct()
)
