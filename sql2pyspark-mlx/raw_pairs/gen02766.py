from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.hire_date").desc(), F.col("c.id").desc())

with_latest = (
    df_departments
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.job_title"), F.col("c.hire_date"))
)

result = (
    with_latest
    .groupBy("location")
    .agg(
        F.sum(F.when(F.col("location") == "New York", F.col("budget")).otherwise(0)).alias("new_york_budget"),
        F.sort_array(F.collect_set("job_title")).alias("job_title_list"),
        F.countDistinct("dept_name").alias("distinct_dept_names"),
    )
)
