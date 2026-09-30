from pyspark.sql import functions as F

recent_employees = (
    df_employees
    .filter(~((F.col("level") == "L1") & (F.col("is_remote") | F.col("level").isin(["L1", "L3"]))))
)

agg = (
    recent_employees
    .groupBy(
        F.when(F.year("hire_date") < 2024, "before_2024").when(F.year("hire_date") == 2024, "2024").otherwise("after_2024").alias("hire_cohort"),
        "dept_id",
    )
    .agg(F.round(F.stddev("salary"), 2).alias("stddev_salary"))
)

result = agg.select("hire_cohort", "dept_id", "stddev_salary")
