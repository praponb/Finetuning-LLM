from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("job_title").isin(["Analyst", "Designer", "Director"]) & ((F.col("job_title") == "Analyst") | ~F.col("level").isin(["L5", "L3", "L2"])))
    .select("salary", "manager_id", "dept_id")
    .orderBy("manager_id")
)
