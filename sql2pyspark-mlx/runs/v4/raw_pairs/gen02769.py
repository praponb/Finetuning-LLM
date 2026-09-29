from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_id").orderBy(F.col("salary").desc())
w2 = Window.partitionBy("dept_id").orderBy("hire_date", "id").rowsBetween(-6, Window.currentRow)

base = (
    df_employees
    .filter(~F.col("is_remote"))
    .select(
        "id",
        "full_name",
        "email",
        "dept_id",
        "job_title",
        "salary",
        "hire_date",
        "is_remote",
        F.trim("job_title").alias("clean_job_title"),
        F.coalesce("level", F.lit("N/A")).alias("level_clean"),
    )
)

result = (
    base
    .select(
        "*",
        F.percent_rank().over(w).alias("salary_percent_rank"),
        F.avg("salary").over(w2).alias("salary_avg_6"),
    )
)
