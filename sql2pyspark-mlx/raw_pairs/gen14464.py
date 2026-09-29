from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("salary", "id").rowsBetween(-2, -1)
w2 = Window.partitionBy(F.date_trunc("YEAR", "hire_date"), "job_title").orderBy("hire_date", "id")
w3 = Window.orderBy("salary", "id")

result = (
    df_employees
    .select(
        "id",
        "full_name",
        "email",
        "manager_id",
        "job_title",
        "is_remote",
        "level",
        F.avg("salary").over(w).alias("salary_avg_2_prior"),
        F.row_number().over(w2).alias("row_seq"),
        F.lead("salary", 1).over(w3).alias("next_salary"),
    )
)
