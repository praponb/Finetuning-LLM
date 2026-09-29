from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.partitionBy("job_title").orderBy("salary_whole", "id").rowsBetween(-3, Window.currentRow)

cleaned = (
    df_employees
    .filter(F.lower("email").like("%ran%"))
    .select(
        "id",
        "email",
        "dept_id",
        "job_title",
        "salary",
        "hire_date",
        "is_remote",
        F.floor("salary").alias("salary_whole"),
        F.coalesce("level", F.lit("N/A")).alias("level_clean"),
    )
)

result = (
    cleaned
    .filter(F.col("salary") <= 109000)
    .select(
        "id",
        "email",
        "dept_id",
        "job_title",
        "hire_date",
        F.count("salary_whole").over(w_running).alias("last4_count_salary_whole"),
        F.avg("salary_whole").over(w_running).alias("last4_avg_salary_whole"),
        F.max("salary_whole").over(w_running).alias("last4_max_salary_whole"),
    )
)
