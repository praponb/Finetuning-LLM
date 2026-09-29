from pyspark.sql import functions as F

result = (
    df_employees
    .filter((F.col("dept_id") == 29) & F.col("is_remote") & F.lower("email").like("%mma%"))
    .groupBy(
        F.when(F.year("hire_date") < 2024, "before_2024").when(F.year("hire_date") == 2024, "2024").otherwise("after_2024").alias("hire_cohort"),
    )
    .agg(
        F.round(F.stddev("salary"), 2).alias("stddev_salary"),
        F.max("salary").alias("max_salary"),
    )
    .orderBy("hire_cohort")
)
