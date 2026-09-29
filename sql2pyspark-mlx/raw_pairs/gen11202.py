from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_name", "location").orderBy("budget", "id").rowsBetween(-2, Window.currentRow)

result = (
    df_departments
    .select(
        "id",
        "dept_name",
        "location",
        F.sum("budget").over(w).alias("last3_sum_budget"),
        F.avg("budget").over(w).alias("last3_avg_budget"),
        F.max("budget").over(w).alias("last3_max_budget"),
    )
)
