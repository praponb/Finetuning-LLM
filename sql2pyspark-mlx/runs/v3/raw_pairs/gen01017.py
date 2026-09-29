from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_id").orderBy("hire_date", "id")
w2 = Window.partitionBy("job_title").orderBy(F.col("salary").desc(), "id")

result = (
    df_employees
    .select(
        "id",
        "dept_id",
        "hire_date",
        "salary",
        "job_title",
        F.sum("salary").over(w).alias("running_salary"),
        F.row_number().over(w2).alias("row_num"),
    )
)
