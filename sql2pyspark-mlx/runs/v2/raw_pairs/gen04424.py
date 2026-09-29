from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_name").orderBy("budget", "id").rowsBetween(-6, Window.currentRow)
w2 = Window.partitionBy("location").orderBy("budget", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_departments
    .filter(F.col("location").isin(["New York", "Chiang Mai", "London"]))
    .select(
        "id",
        "dept_name",
        "budget",
        "location",
        F.round(F.avg("budget").over(w), 2).alias("budget_moving_avg_7"),
        F.sum("budget").over(w2).alias("running_budget"),
    )
    .orderBy("budget_moving_avg_7")
)
