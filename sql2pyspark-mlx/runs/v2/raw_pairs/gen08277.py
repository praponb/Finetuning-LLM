from pyspark.sql import functions as F

src = (
    df_employees
    .filter((F.col("manager_id") == 21) & ((F.col("dept_id") == 12) | (F.col("salary") <= 57000)))
)

employees_stats = (
    src
    .groupBy("manager_id")
    .agg(
        F.max("hire_date").alias("latest_hire_date"),
        F.round(F.stddev("salary"), 2).alias("stddev_salary"),
    )
)

result = (
    employees_stats
    .filter(F.col("stddev_salary") > 46)
    .select("manager_id", "latest_hire_date")
    .orderBy(F.col("manager_id").desc())
)
