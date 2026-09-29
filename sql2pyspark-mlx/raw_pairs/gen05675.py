from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.orderBy("salary", "id")
w2 = Window.partitionBy("job_title", "manager_id")
w3 = Window.partitionBy(F.date_trunc("MONTH", "hire_date"), "level").orderBy(F.col("salary").desc(), "id")
w4 = Window.partitionBy(F.date_trunc("YEAR", "hire_date"), "manager_id").orderBy(F.col("salary").desc())
w5 = Window.orderBy("hire_date", "id")

result = (
    df_employees
    .select(
        "id",
        "full_name",
        "email",
        "job_title",
        "hire_date",
        "is_remote",
        "level",
        F.lead("salary", 1).over(w_grp).alias("next_salary"),
        F.round((F.col("salary") / F.sum("salary").over(w2)), 4).alias("salary_share"),
        F.ntile(3).over(w3).alias("salary_bucket"),
        F.dense_rank().over(w4).alias("salary_dense_rank"),
        F.lag("salary", 1).over(w5).alias("prev_salary"),
    )
)
