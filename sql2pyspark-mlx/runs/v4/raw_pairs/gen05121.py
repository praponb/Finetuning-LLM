from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("manager_id").orderBy(F.col("salary").desc())
w2 = Window.partitionBy("level", "job_title").orderBy("salary")

result = (
    df_employees
    .filter(F.col("level").isNull())
    .select(
        "id",
        "manager_id",
        "salary",
        "level",
        "job_title",
        F.rank().over(w).alias("rnk"),
        F.percent_rank().over(w2).alias("salary_pct_rank"),
    )
)
