from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level")
w2 = Window.partitionBy("job_title", "dept_id").orderBy("salary")
w3 = Window.partitionBy("job_title")

result = (
    df_employees
    .select(
        "id",
        "full_name",
        "job_title",
        "salary",
        "hire_date",
        F.stddev_samp("salary").over(w).alias("level_stddev_samp_salary"),
        F.rank().over(w2).alias("salary_rank"),
        F.max("salary").over(w3).alias("job_title_max_salary"),
    )
)
