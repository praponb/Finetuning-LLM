from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("job_title").isin(["Intern", "Director", "Designer"]) & F.col("email").like("%@company.co"))
    .agg(
        F.round(F.avg("salary"), 2).alias("avg_salary"),
        F.sort_array(F.collect_set("level")).alias("level_list"),
    )
)
