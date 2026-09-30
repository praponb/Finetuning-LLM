from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("salary").between(118000, 137000) & F.col("manager_id").isin([5, 11]))
    .groupBy(
        F.when(F.year("hire_date") < 2024, "before_2024").when(F.year("hire_date") == 2024, "2024").otherwise("after_2024").alias("hire_cohort"),
        F.when(F.col("salary") >= 138000, "high").when(F.col("salary") >= 75000, "mid").otherwise("low").alias("salary_bucket"),
    )
    .agg(F.round(F.avg("salary"), 2).alias("avg_salary"))
    .filter(F.col("avg_salary") > 25)
    .orderBy(F.col("salary_bucket").desc(), "hire_cohort")
)
