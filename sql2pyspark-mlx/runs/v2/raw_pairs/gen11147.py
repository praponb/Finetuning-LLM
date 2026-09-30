from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("location", "dept_name").orderBy(F.col("budget").desc())
w2 = Window.partitionBy("location").orderBy("budget", "id").rowsBetween(-3, Window.currentRow)

result = (
    df_departments
    .filter(F.col("location") == "Bangkok")
    .select(
        "id",
        "location",
        "dept_name",
        "budget",
        F.percent_rank().over(w).alias("budget_pct_rank"),
        F.round(F.avg("budget").over(w2), 2).alias("budget_moving_avg_4"),
    )
)
