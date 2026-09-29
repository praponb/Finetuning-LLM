from pyspark.sql import functions as F

result = (
    df_employees
    .withColumn("quarter_start", F.date_trunc("quarter", "hire_date"))
    .groupingSets(
        [["manager_id", "quarter_start"], ["manager_id"], []],
        "manager_id",
        "quarter_start",
    )
    .agg(
        F.max(F.when(F.col("job_title") == "Intern", F.col("salary"))).alias("intern_max_salary"),
        F.count(F.when(F.col("job_title") == "Engineer", 1)).alias("engineer_rows"),
        F.round(F.avg("salary"), 2).alias("avg_salary"),
    )
    .orderBy(F.col("intern_max_salary").desc())
)
