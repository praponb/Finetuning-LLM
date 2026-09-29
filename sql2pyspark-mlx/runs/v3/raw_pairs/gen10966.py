from pyspark.sql import functions as F

result = (
    df_customers
    .select("email", F.lit("customer").alias("source"))
    .unionByName(
        df_employees
        .filter(~F.col("is_remote"))
        .select("email", F.lit("employee").alias("source"))
    )
    .orderBy(F.col("email").desc())
)
