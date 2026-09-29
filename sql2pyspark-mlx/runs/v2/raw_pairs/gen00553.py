from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_name").orderBy(F.col("budget").desc())
w2 = Window.partitionBy("location").orderBy("budget", "id")

result = (
    df_departments
    .select(
        "id",
        "dept_name",
        "budget",
        "location",
        F.dense_rank().over(w).alias("dense_rnk"),
        F.lag("budget", 1).over(w2).alias("prev_budget"),
    )
)
