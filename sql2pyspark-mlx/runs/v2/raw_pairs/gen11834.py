from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("budget", "id").rowsBetween(-6, Window.currentRow)

result = (
    df_departments
    .select(
        "id",
        "budget",
        F.round(F.avg("budget").over(w), 2).alias("budget_moving_avg_7"),
    )
)
