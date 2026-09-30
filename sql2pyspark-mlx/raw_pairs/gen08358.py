from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("is_active"))
    .select("email", F.lit("customer").alias("kind"))
    .unionByName(df_employees.select("email", F.lit("employee").alias("kind")))
    .distinct()
    .orderBy("email")
)
