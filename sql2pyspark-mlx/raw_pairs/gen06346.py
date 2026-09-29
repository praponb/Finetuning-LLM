from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_name")
w2 = Window.partitionBy("dept_name").orderBy(F.col("budget").desc(), "id")

result = (
    df_departments
    .select(
        "id",
        "dept_name",
        "budget",
        F.avg("budget").over(w).alias("dept_name_avg_budget"),
        F.ntile(2).over(w2).alias("bucket"),
    )
)
