from pyspark.sql import functions as F

result = (
    df_customers
    .select("email", F.lit("customer").alias("source"))
    .unionByName(
        df_employees
        .filter(F.col("dept_id").isin([9, 21]))
        .select("email", F.lit("employee").alias("source"))
    )
    .distinct()
    .orderBy("source")
)
