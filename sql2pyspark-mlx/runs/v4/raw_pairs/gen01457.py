from pyspark.sql import functions as F

src = (
    df_employees
    .filter(F.col("manager_id") == 4)
    .withColumn("hire_date_plus_88d", F.date_add("hire_date", 88))
)

summary = (
    src
    .groupBy(F.year("hire_date_plus_88d").alias("hire_plus_88d_year"))
    .agg(
        (F.max("salary") - F.min("salary")).alias("salary_range"),
        F.round(F.stddev("salary"), 2).alias("stddev_salary"),
    )
)

result = (
    summary
    .select("hire_plus_88d_year", "salary_range")
    .orderBy("salary_range", F.col("hire_plus_88d_year").desc())
)
