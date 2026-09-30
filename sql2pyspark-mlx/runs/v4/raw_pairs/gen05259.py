from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("hire_date").between("2024-02-09", "2024-05-01") & (F.col("manager_id") == 40))
    .groupBy(
        F.when(F.year("hire_date") < 2024, "before_2024").when(F.year("hire_date") == 2024, "2024").otherwise("after_2024").alias("hire_cohort"),
    )
    .agg(
        F.count(F.when(F.col("level") == "L2", 1)).alias("l2_count"),
        F.count("*").alias("num_employees"),
        F.min("salary").alias("min_salary"),
    )
    .filter(F.col("l2_count") < 2)
    .orderBy("hire_cohort")
)
