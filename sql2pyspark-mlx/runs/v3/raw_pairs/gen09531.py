from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("job_title").orderBy("hire_date")
w2 = Window.orderBy("hire_date", "id")

result = (
    df_employees
    .select(
        "id",
        "job_title",
        "hire_date",
        "salary",
        F.dense_rank().over(w).alias("hire_date_dense_rank"),
        F.sum("salary").over(w2).alias("running_salary"),
    )
    .orderBy(F.col("id").desc())
)
