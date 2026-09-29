from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("salary")
w2 = Window.partitionBy("manager_id", "dept_id")
w3 = Window.orderBy(F.col("salary").desc(), "id")
w4 = Window.partitionBy(F.date_trunc("week", "hire_date"), "manager_id")

result = (
    df_employees
    .select(
        "id",
        "full_name",
        "manager_id",
        "job_title",
        "salary",
        "level",
        F.cume_dist().over(w).alias("salary_cume_dist"),
        F.avg("salary").over(w2).alias("manager_id_avg_salary"),
        F.row_number().over(w3).alias("event_seq"),
        F.max("salary").over(w4).alias("hire_date_period_max_salary"),
    )
)
