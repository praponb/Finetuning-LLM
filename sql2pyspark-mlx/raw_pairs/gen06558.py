from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("job_title").orderBy("salary")

result = (
    df_employees
    .filter(F.col("job_title").isNotNull())
    .select(
        "id",
        "job_title",
        "salary",
        F.percent_rank().over(w).alias("salary_pct_rank"),
    )
    .orderBy("salary_pct_rank")
)
