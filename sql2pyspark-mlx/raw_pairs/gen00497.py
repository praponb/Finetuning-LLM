from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("job_title")
w2 = Window.partitionBy("level").orderBy("hire_date")

result = (
    df_employees
    .select(
        "id",
        "job_title",
        "salary",
        "level",
        "hire_date",
        F.round((F.col("salary") / F.sum("salary").over(w)), 3).alias("salary_share"),
        F.rank().over(w2).alias("hire_date_rank"),
    )
)
