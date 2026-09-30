from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("location").orderBy("budget", "id").rowsBetween(-3, Window.currentRow)

result = (
    df_departments
    .filter(F.col("budget").between(1382000, 3901000))
    .select(
        "id",
        "location",
        "budget",
        F.round(F.avg("budget").over(w), 2).alias("budget_moving_avg_4"),
    )
)
