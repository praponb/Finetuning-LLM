from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("job_title")

result = (
    df_employees
    .select(
        "id",
        "job_title",
        "salary",
        F.max("salary").over(w).alias("job_title_max_salary"),
    )
    .orderBy(F.col("id").desc())
)
