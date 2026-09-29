from pyspark.sql import functions as F

result = (
    df_employees
    .groupBy("dept_id")
    .agg(
        F.count(F.when(F.col("level") == "L3", 1)).alias("l3_count"),
        F.count(F.when(F.col("job_title") == "Manager", 1)).alias("manager_count"),
    )
    .filter(F.col("l3_count") > 4)
    .select("dept_id", "l3_count", "manager_count")
    .orderBy("l3_count", F.col("manager_count").desc(), "dept_id")
    .limit(5)
)
