from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("job_title")

result = (
    df_employees
    .select(
        "id",
        "job_title",
        "salary",
        F.round((F.col("salary") / F.sum("salary").over(w)), 3).alias("salary_share"),
    )
)
