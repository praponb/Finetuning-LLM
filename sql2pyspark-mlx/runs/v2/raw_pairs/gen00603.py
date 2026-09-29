from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.upper("full_name").like("%VY %"))
    .groupBy("level")
    .agg(
        F.sum(F.when(F.col("job_title") == "Intern", F.col("salary")).otherwise(0)).alias("intern_salary"),
        F.sort_array(F.collect_set("job_title")).alias("job_title_list"),
    )
    .filter(F.col("intern_salary") >= 74)
    .select("level", "intern_salary", "job_title_list")
)
