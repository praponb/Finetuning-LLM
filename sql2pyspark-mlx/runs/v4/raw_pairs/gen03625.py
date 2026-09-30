from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_id", "job_title").orderBy("salary")
w2 = Window.partitionBy("dept_id").orderBy("dedup_rank", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_hist = Window.partitionBy("level").orderBy(F.col("salary").desc())
w4 = Window.partitionBy("dept_id")
w5 = Window.partitionBy("dept_id", "manager_id")
w6 = Window.partitionBy("level", "job_title").orderBy("hire_date", "id").rowsBetween(-5, Window.currentRow)

deduped = (
    df_employees
    .withColumn("dedup_rank", F.rank().over(w))
    .filter(F.col("dedup_rank") == 1)
)

with_windows = (
    deduped
    .select(
        "*",
        F.last("level", ignorenulls=True).over(w2).alias("last_known_level"),
        F.cume_dist().over(w_hist).alias("salary_cume_dist"),
    )
)

joined = (
    with_windows
    .alias("a")
    .join(df_employees.alias("d"), F.col("a.manager_id") == F.col("d.id"), "inner")
    .select("a.*", F.col("d.job_title").alias("employee_job_title"))
)

enriched = (
    joined
    .select(
        "*",
        F.min("salary_cume_dist").over(w4).alias("dept_id_min_salary_cume_dist"),
        F.sum("salary").over(w5).alias("dept_id_sum_salary"),
        F.avg("salary_cume_dist").over(w6).alias("salary_cume_dist_avg_5"),
    )
)

result = (
    enriched
    .select(
        "id",
        "full_name",
        "dept_id",
        "job_title",
        "hire_date",
        "last_known_level",
        "employee_job_title",
        "dept_id_min_salary_cume_dist",
        "dept_id_sum_salary",
        F.when(F.col("job_title").rlike("^(Manager|Analyst)$"), "CORE").otherwise("EXTENDED").alias("job_title_zone"),
    )
)
