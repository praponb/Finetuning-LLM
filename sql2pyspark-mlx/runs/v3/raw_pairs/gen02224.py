from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("job_title").isin(["Analyst", "Director"]) | F.col("job_title").isin(["Designer", "Engineer"]))
    .groupBy("manager_id")
    .agg(
        F.round(F.stddev("salary"), 2).alias("stddev_salary"),
        F.count(F.when(F.col("level") == "L3", 1)).alias("l3_count"),
    )
)
