from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_name")
w2 = Window.orderBy("budget", "id").rowsBetween(-3, Window.currentRow)

result = (
    df_departments
    .select(
        "id",
        "dept_name",
        "budget",
        F.round((F.col("budget") / F.sum("budget").over(w)), 3).alias("budget_share"),
        F.round(F.avg("budget").over(w2), 2).alias("budget_moving_avg_4"),
    )
)
