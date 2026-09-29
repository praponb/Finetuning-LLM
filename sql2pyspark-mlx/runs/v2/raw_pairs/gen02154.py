from pyspark.sql import functions as F

result = (
    df_customers
    .select("email", F.lit("customer").alias("source"))
    .unionByName(
        df_employees
        .filter(F.col("manager_id") == 25)
        .select("email", F.lit("employee").alias("source"))
    )
)
