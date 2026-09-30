from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("job_title")

result = (
    df_employees
    .filter(F.col("salary") >= 122000)
    .select(
        "id",
        "job_title",
        "salary",
        F.sum("salary").over(w).alias("job_title_sum_salary"),
    )
    .orderBy(F.col("salary").desc())
)
