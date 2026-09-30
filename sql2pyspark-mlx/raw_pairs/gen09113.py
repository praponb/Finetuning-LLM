from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("location", "dept_name")
w2 = Window.partitionBy("dept_name").orderBy("budget", "id")

result = (
    df_departments
    .select(
        "*",
        F.min("budget").over(w).alias("location_min_budget"),
        F.lead("budget", 1).over(w2).alias("next_budget"),
    )
)
