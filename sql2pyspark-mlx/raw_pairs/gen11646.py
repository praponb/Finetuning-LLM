from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("age").isNull())
    .select(F.lit("customer").alias("source"), "email")
    .unionByName(
        df_employees
        .filter(F.lower("full_name").like("%oah%"))
        .select(F.lit("employee").alias("source"), "email")
    )
    .distinct()
)
