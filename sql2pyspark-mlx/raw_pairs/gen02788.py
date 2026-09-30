from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("location").orderBy("budget", "id")
w2 = Window.partitionBy("dept_name").orderBy("budget", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

result = (
    df_departments
    .filter(F.col("dept_name").isin(["Engineering", "Legal", "Marketing"]))
    .select(
        "id",
        "location",
        "budget",
        "dept_name",
        F.row_number().over(w).alias("rn"),
        F.last("location").over(w2).alias("last_location"),
    )
)
