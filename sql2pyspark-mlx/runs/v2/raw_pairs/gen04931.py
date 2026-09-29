from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.upper("full_name").like("%RAN%") & ~F.col("job_title").isin(["Intern", "Engineer", "Director"]))
    .select("id", "level", F.length("job_title").alias("job_title_length"))
)
