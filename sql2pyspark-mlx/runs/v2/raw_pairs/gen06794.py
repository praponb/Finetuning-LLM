from pyspark.sql import functions as F

result = (
    df_employees
    .groupBy("job_title")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("job_title") == "Engineer", 1).otherwise(0))) / F.count("*")), 1).alias("pct_engineer"),
        F.min("salary").alias("min_salary"),
        F.max("salary").alias("max_salary"),
    )
    .filter(F.col("max_salary") > 106000)
    .drop("max_salary")
)
