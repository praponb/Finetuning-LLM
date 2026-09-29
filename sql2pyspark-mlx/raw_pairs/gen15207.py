from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.hire_date").desc(), F.col("c.id").desc())

latest_match = (
    df_departments
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.id", "s.dept_name", F.col("c.job_title"), F.col("c.salary"))
)

result = (
    latest_match
    .groupBy("dept_name", "job_title")
    .agg(
        F.round(F.avg("salary"), 2).alias("avg_salary"),
        F.sum(F.when(F.col("dept_name") == "Engineering", F.col("salary"))).alias("engineering_total_salary"),
        F.sort_array(F.collect_set("dept_name")).alias("dept_name_list"),
        F.percentile_approx("salary", 0.5).alias("salary_p50"),
        F.count(F.when(F.col("job_title") == "Designer", 1)).alias("designer_count"),
    )
)
