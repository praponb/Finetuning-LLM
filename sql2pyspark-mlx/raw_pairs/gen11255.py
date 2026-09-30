from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("manager_id").orderBy(F.col("salary").desc(), "id")

deduped = (
    df_employees
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "id",
        "full_name",
        "email",
        "dept_id",
        "manager_id",
        "job_title",
        "salary",
        "level",
    )
)

result = (
    deduped
    .groupBy("dept_id")
    .agg(
        F.percentile_approx("salary", [0.25, 0.5, 0.75]).alias("salary_quartiles"),
        F.count(F.when(F.col("job_title") == "Analyst", 1)).alias("analyst_count"),
        F.count(F.when(F.col("job_title") == "Analyst", 1)).alias("analyst_count_2"),
    )
)
