from pyspark.sql import functions as F

src = df_employees.filter(~F.col("level").isin(["L2", "L3", "L1"]))

summary = (
    src
    .groupBy("job_title")
    .agg(
        F.round(F.stddev("salary"), 2).alias("stddev_salary"),
        F.min("salary").alias("min_salary"),
    )
)

result = (
    summary
    .filter(F.col("stddev_salary") >= 49)
    .select("job_title", "stddev_salary", "min_salary")
    .orderBy("min_salary")
)
