from pyspark.sql import functions as F

result = (
    df_employees
    .select("hire_date", F.lit("employee").alias("source"))
    .unionByName(
        df_customers
        .filter(F.col("country").isin(["DE", "UK", "US"]))
        .select(F.col("signup_date").alias("hire_date"), F.lit("customer").alias("source"))
    )
    .orderBy("hire_date")
)
