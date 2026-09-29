from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("job_title", "manager_id").orderBy("salary")
w2 = Window.partitionBy("manager_id").orderBy("hire_date", "id").rowsBetween(-3, -1)

cleaned = (
    df_employees
    .filter(F.col("is_remote"))
    .withColumn("hire_date_month_end", F.last_day("hire_date"))
)

result = (
    cleaned
    .filter(F.col("level").isNull())
    .select(
        "id",
        "dept_id",
        "manager_id",
        "job_title",
        "salary",
        "hire_date",
        "is_remote",
        "level",
        F.cume_dist().over(w).alias("salary_cume_dist"),
        F.avg("salary").over(w2).alias("salary_avg_3_prior"),
    )
)
