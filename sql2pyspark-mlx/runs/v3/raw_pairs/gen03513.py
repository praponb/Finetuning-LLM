from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("location").orderBy("budget", "id").rowsBetween(-3, Window.currentRow)
w2 = Window.partitionBy("dept_name").orderBy("budget", "id")

result = (
    df_departments
    .filter(F.col("location").isin(["Berlin", "Bangkok", "London"]))
    .select(
        "id",
        "location",
        "budget",
        "dept_name",
        F.round(F.avg("budget").over(w), 2).alias("budget_moving_avg_4"),
        F.lag("budget", 1).over(w2).alias("prev_budget"),
    )
    .orderBy(F.col("budget").desc())
)
