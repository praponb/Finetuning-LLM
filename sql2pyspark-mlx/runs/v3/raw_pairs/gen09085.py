from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("location", "dept_name").orderBy("budget", "id")

result = (
    df_departments
    .filter(F.col("budget") > 1710000)
    .select(
        "id",
        "location",
        "dept_name",
        "budget",
        F.ntile(10).over(w).alias("bucket"),
    )
)
