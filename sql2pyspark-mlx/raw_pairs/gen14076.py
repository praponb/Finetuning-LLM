from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("name").isNotNull())
    .select("name", F.lit("customer").alias("source"))
    .union(
        df_employees
        .filter(F.col("full_name").isNotNull())
        .select("full_name", F.lit("employee").alias("source"))
    )
    .distinct()
)
