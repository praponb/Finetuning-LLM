from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.salary").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("location").orderBy(F.col("employee_salary").desc_nulls_last(), "id")

latest_match = (
    df_departments
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.dept_name",
        "s.location",
        F.col("c.job_title").alias("employee_job_title"),
        F.col("c.salary").alias("employee_salary"),
    )
)

deduped = (
    latest_match
    .withColumn("row_num", F.row_number().over(w2))
    .filter(F.col("row_num") == 1)
)

deduped_departments = (
    deduped
    .withColumn("row_num_2", F.row_number().over(w2))
    .filter(F.col("row_num_2") == 1)
)

eligible = (
    deduped_departments
    .alias("s")
    .join(df_employees.alias("x"), (F.col("x.dept_id") == F.col("s.id")) & (F.col("x.dept_id") == 11), "left_semi")
)

result = (
    eligible
    .withColumn("employee_salary_tier", F.when(F.col("employee_salary") >= 139000, "HIGH").otherwise("LOW"))
    .cube("location", "employee_job_title", "employee_salary_tier")
    .agg(
        F.count_if(F.col("dept_name") == "Marketing").alias("marketing_events"),
        F.percentile_approx("employee_salary", 0.5).alias("employee_salary_p50"),
        F.grouping_id().alias("grouping_level"),
    )
    .filter(F.col("marketing_events") < 1000)
    .orderBy("location")
)
