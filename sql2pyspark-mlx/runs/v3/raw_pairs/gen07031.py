from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_name").orderBy("budget", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

result = (
    df_departments
    .select(
        "id",
        "dept_name",
        "budget",
        "location",
        F.last("location").over(w).alias("last_location"),
    )
)
