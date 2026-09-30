from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("location").orderBy("budget", "id").rowsBetween(-6, Window.currentRow)
w2 = Window.partitionBy("location")

result = (
    df_departments
    .select(
        "id",
        "location",
        "budget",
        F.round(F.avg("budget").over(w), 2).alias("budget_moving_avg_7"),
        F.round((F.col("budget") / F.sum("budget").over(w2)), 3).alias("budget_share"),
    )
    .orderBy(F.col("budget_share").desc(), F.col("budget").desc())
)
