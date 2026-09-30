from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level", "job_title")

result = (
    df_employees
    .filter(F.col("level") != "L3")
    .select(
        "id",
        "level",
        "job_title",
        "salary",
        F.min("salary").over(w).alias("level_min_salary"),
    )
    .orderBy("level")
)
