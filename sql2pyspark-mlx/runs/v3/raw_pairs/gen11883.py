from pyspark.sql import functions as F

result = (
    df_employees
    .groupBy(F.when(F.year("hire_date") < 2024, "before_2024").when(F.year("hire_date") == 2024, "2024").otherwise("after_2024").alias("hire_cohort"))
    .pivot("job_title", ["Analyst", "Manager", "Director", "Engineer"])
    .agg(F.avg("salary"))
    .orderBy("hire_cohort")
)
