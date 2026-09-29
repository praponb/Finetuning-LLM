from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.month("hire_date"), "job_title").orderBy("hire_date")

result = (
    df_employees
    .filter(F.col("level") != "L5")
    .select(
        "id",
        "hire_date",
        "job_title",
        F.percent_rank().over(w).alias("hire_date_pct_rank"),
    )
    .orderBy("hire_date_pct_rank")
)
