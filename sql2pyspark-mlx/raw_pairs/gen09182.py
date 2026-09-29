from pyspark.sql import functions as F

result = (
    df_employees
    .filter(~(F.col("full_name").like("%Lee%") & F.col("email").like("%@startup.io") & F.col("salary").between(97000, 118000)))
    .groupBy("job_title")
    .agg(
        F.max("salary").alias("max_salary"),
        F.sum(F.when(F.col("level") == "L3", F.col("salary")).otherwise(0)).alias("l3_salary"),
    )
)
