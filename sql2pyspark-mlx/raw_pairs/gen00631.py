from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_name")
w2 = Window.orderBy("budget", "id").rowsBetween(-6, Window.currentRow)

result = (
    df_departments
    .select(
        "id",
        "dept_name",
        "location",
        F.min("budget").over(w).alias("dept_name_min_budget"),
        F.round((F.col("budget") / F.sum("budget").over(w)), 4).alias("budget_share"),
        F.avg("budget").over(w2).alias("budget_avg_6"),
    )
)
