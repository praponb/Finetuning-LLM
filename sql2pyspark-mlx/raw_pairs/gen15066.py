from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.partitionBy("location").orderBy("budget", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_departments
    .select(
        "*",
        F.avg("budget").over(w_grp).alias("running_avg_budget"),
        F.min("budget").over(w_grp).alias("running_min_budget"),
        F.count("budget").over(w_grp).alias("running_count_budget"),
    )
)
