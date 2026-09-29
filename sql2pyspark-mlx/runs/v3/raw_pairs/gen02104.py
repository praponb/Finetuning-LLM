from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("age") == 36)
    .select("email", F.lit("customer").alias("source"))
    .unionByName(
        df_employees
        .filter(F.col("dept_id") == 6)
        .select("email", F.lit("employee").alias("source"))
    )
)
