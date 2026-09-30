from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_name", "location").orderBy("budget", "id").rowsBetween(-6, Window.currentRow)

result = (
    df_departments
    .select(
        "id",
        "dept_name",
        "location",
        "budget",
        F.round(F.avg("budget").over(w), 2).alias("budget_moving_avg_7"),
    )
    .orderBy(F.col("budget_moving_avg_7").desc())
)
