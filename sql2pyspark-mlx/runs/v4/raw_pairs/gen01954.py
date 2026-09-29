from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.salary").desc(), F.col("c.id").desc())

with_latest = (
    df_departments
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.level").alias("employee_level"))
)

result = (
    with_latest
    .groupBy(
        F.when(F.col("budget") >= 4091000, "LARGE").otherwise("SMALL").alias("budget_tier"),
        "location",
    )
    .agg(
        F.size(F.collect_set("location")).alias("num_locations"),
        F.avg(F.when(F.col("dept_name") == "HR", F.col("budget"))).alias("hr_avg_budget"),
        F.avg(F.when(F.col("dept_name") == "HR", F.col("budget"))).alias("hr_avg_budget_2"),
        F.count_if(F.col("location") == "Tokyo").alias("tokyo_events"),
        F.stddev_samp("budget").alias("budget_stddev"),
        F.sum(F.when(F.col("dept_name") == "Engineering", F.col("budget")).otherwise(0)).alias("engineering_budget"),
    )
    .orderBy(F.col("budget_stddev").desc_nulls_first())
)
