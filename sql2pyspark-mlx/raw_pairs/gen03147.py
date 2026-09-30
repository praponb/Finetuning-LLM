from pyspark.sql import functions as F

result = (
    df_customers
    .select("email", F.lit("customer").alias("source"))
    .union(
        df_employees
        .filter(F.col("hire_date") < "2023-04-12")
        .select("level", F.lit("employee").alias("source"))
    )
    .orderBy("email")
)
