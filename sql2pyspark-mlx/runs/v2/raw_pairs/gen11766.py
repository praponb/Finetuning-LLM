from pyspark.sql import functions as F

result = (
    df_employees
    .filter(~F.col("job_title").isin(["Engineer", "Manager", "Designer"]) & (F.length("email") > 14))
    .groupBy("job_title", F.year("hire_date").alias("hire_year"))
    .agg(
        F.max("salary").alias("max_salary"),
        F.round(F.stddev("salary"), 2).alias("stddev_salary"),
    )
    .filter(F.col("max_salary") < 148000)
    .orderBy(F.col("stddev_salary").desc(), F.col("hire_year").desc())
)
