from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_name", "location").orderBy("budget", "id")
w2 = Window.partitionBy("location").orderBy(F.col("budget").desc(), "id")

result = (
    df_departments
    .select(
        "id",
        "dept_name",
        "location",
        "budget",
        F.lag("budget", 1).over(w).alias("prev_budget"),
        F.ntile(4).over(w2).alias("bucket"),
    )
)
