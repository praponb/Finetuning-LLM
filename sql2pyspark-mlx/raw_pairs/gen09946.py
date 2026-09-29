from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level")
w2 = Window.partitionBy("manager_id").orderBy(F.col("salary").desc())

result = (
    df_employees
    .select(
        "id",
        "level",
        "salary",
        "manager_id",
        F.avg("salary").over(w).alias("level_avg_salary"),
        F.percent_rank().over(w2).alias("salary_pct_rank"),
    )
)
