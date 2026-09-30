from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("salary", "id")
w2 = Window.partitionBy("job_title")

result = (
    df_employees
    .filter(F.col("job_title").isNotNull())
    .select(
        "id",
        "salary",
        "job_title",
        F.row_number().over(w).alias("rn"),
        F.avg("salary").over(w2).alias("job_title_avg_salary"),
    )
    .orderBy(F.col("job_title_avg_salary").desc())
)
