from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("location", "dept_name")
w2 = Window.partitionBy("location").orderBy("budget", "id")

result = (
    df_departments
    .select(
        "id",
        "location",
        "dept_name",
        "budget",
        F.max("budget").over(w).alias("location_max_budget"),
        F.sum("budget").over(w2).alias("running_budget"),
    )
)
