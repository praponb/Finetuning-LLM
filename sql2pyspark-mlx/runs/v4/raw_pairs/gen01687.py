from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_name").orderBy("budget", "id").rowsBetween(-2, Window.currentRow)

result = (
    df_departments
    .select(
        "id",
        "dept_name",
        "budget",
        F.round(F.avg("budget").over(w), 2).alias("budget_moving_avg_3"),
    )
)
