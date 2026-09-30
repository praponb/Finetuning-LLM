from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("job_title").orderBy("hire_date")
w2 = Window.partitionBy("dept_id").orderBy(F.col("salary").desc(), "id")

result = (
    df_employees
    .filter(F.col("job_title").isin(["Director", "Intern"]))
    .select(
        "id",
        "job_title",
        "hire_date",
        "dept_id",
        "salary",
        F.dense_rank().over(w).alias("hire_date_dense_rank"),
        F.row_number().over(w2).alias("rn"),
    )
)
