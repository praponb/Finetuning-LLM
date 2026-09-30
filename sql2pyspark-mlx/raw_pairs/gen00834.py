from pyspark.sql import functions as F

valid_employees = df_employees.filter(~F.col("job_title").isin(["Intern", "Engineer", "Analyst"]))

agg = (
    valid_employees
    .groupBy("job_title")
    .agg(
        F.sum(F.when(F.col("level") == "L4", F.col("salary")).otherwise(0)).alias("l4_salary"),
        F.round(((100 * F.sum(F.when(F.col("level") == "L3", 1).otherwise(0))) / F.count("*")), 1).alias("pct_l3"),
    )
)

result = (
    agg
    .select("job_title", "l4_salary", "pct_l3")
    .orderBy("job_title")
)
