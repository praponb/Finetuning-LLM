from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_name").orderBy("budget", "id")

result = (
    df_departments
    .filter(F.col("location") == "Chiang Mai")
    .select(
        "id",
        "dept_name",
        "budget",
        F.sum("budget").over(w).alias("running_budget"),
    )
)
