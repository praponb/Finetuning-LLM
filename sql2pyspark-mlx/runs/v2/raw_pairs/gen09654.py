from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("location").orderBy("budget", "id").rowsBetween(-2, Window.currentRow)

result = (
    df_departments
    .select(
        "id",
        "location",
        "budget",
        F.round(F.avg("budget").over(w), 2).alias("budget_moving_avg_3"),
    )
)
