from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("location").orderBy(F.col("budget").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

result = (
    df_departments
    .select(
        "id",
        "location",
        "budget",
        "dept_name",
        F.last("dept_name").over(w).alias("last_dept_name"),
    )
)
