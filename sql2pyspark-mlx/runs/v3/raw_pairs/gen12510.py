from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level", "job_title")

result = (
    df_employees
    .select(
        "id",
        "level",
        "job_title",
        "salary",
        F.round((F.col("salary") / F.sum("salary").over(w)), 3).alias("salary_share"),
    )
    .orderBy("level")
)
