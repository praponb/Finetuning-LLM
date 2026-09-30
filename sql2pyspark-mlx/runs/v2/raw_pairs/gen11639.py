from pyspark.sql import functions as F

valid_employees = (
    df_employees
    .filter(~F.col("is_remote") | F.col("manager_id").isNull())
    .withColumn("is_remote_label", F.when(F.col("is_remote"), "Yes").otherwise("No"))
)

summary = (
    valid_employees
    .groupBy("job_title")
    .agg(
        F.max("hire_date").alias("last_hire_date"),
        F.round(F.stddev("salary"), 2).alias("stddev_salary"),
    )
)

result = (
    summary
    .filter(F.col("stddev_salary") >= 77)
    .select("job_title", "last_hire_date", "stddev_salary")
    .orderBy(F.col("stddev_salary").desc(), "last_hire_date")
)
