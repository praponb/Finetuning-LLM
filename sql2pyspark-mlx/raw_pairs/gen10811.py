from pyspark.sql import functions as F

result = (
    df_employees
    .groupBy(
        F.year("hire_date").alias("hire_year"),
        F.when(F.year("hire_date") < 2024, "before_2024").when(F.year("hire_date") == 2024, "2024").otherwise("after_2024").alias("hire_cohort"),
    )
    .agg(F.min("salary").alias("min_salary"))
    .orderBy("hire_cohort", "hire_year")
    .limit(1)
)
