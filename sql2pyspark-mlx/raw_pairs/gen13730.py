from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy("dept_name", "location").orderBy("budget", "id").rowsBetween(-5, Window.currentRow)
w2 = Window.partitionBy("dept_name", "location")

result = (
    df_departments
    .select(
        "id",
        "dept_name",
        "location",
        "budget",
        F.avg("budget").over(w_recent).alias("budget_avg_5"),
        F.round((F.col("budget") / F.sum("budget").over(w2)), 4).alias("budget_share"),
    )
)
