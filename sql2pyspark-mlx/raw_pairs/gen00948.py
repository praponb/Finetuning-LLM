from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("city").isNotNull())
    .select(F.lit("customer").alias("origin"), "name")
    .union(
        df_employees
        .filter(F.col("salary") <= 100000)
        .select(F.lit("employee").alias("origin"), "full_name")
    )
    .distinct()
)
