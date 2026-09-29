from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("location").orderBy("budget", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

result = (
    df_departments
    .filter(~F.col("dept_name").isin(["Legal", "Finance"]))
    .select(
        "id",
        "location",
        "budget",
        "dept_name",
        F.last("dept_name").over(w).alias("last_dept_name"),
    )
    .orderBy(F.col("id").desc())
)
