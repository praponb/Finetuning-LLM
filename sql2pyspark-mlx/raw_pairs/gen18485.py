from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.date_trunc("WEEK", "hire_date"), "level").orderBy(F.col("salary").desc(), "id")
w2 = Window.partitionBy("job_title", "dept_id").orderBy("hire_date", "id")

result = (
    df_employees
    .select(
        "id",
        "hire_date",
        "level",
        "salary",
        "job_title",
        "dept_id",
        F.row_number().over(w).alias("row_num"),
        F.lag("salary", 1).over(w2).alias("prev_salary"),
    )
)
