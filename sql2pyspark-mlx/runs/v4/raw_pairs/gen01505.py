from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("salary").between(60000, 137000))
    .select("hire_date")
    .unionByName(
        df_subscriptions
        .filter(F.year("end_date") == 2024)
        .select(F.col("end_date").alias("hire_date"))
    )
)
