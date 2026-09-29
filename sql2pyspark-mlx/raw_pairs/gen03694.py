from pyspark.sql import functions as F

result = (
    df_employees
    .select(F.lit("employee").alias("kind"), F.col("hire_date").alias("event_date"))
    .unionByName(
        df_customers
        .filter(F.col("age") != 40)
        .select(F.lit("customer").alias("kind"), F.col("signup_date").alias("event_date"))
    )
)
