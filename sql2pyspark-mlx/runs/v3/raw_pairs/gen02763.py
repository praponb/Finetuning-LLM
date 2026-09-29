from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("location").orderBy("budget", "id")
w2 = Window.orderBy("budget", "id").rowsBetween(-2, Window.currentRow)

result = (
    df_departments
    .select(
        "id",
        "location",
        "budget",
        F.lag("budget", 1).over(w).alias("prev_budget"),
        F.round(F.avg("budget").over(w2), 2).alias("budget_moving_avg_3"),
    )
)
