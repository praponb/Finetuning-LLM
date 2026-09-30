from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("location").orderBy("budget", "id")
w2 = Window.partitionBy("dept_name").orderBy("budget", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

result = (
    df_departments
    .select(
        "id",
        "location",
        "budget",
        "dept_name",
        F.lead("budget", 1, 0).over(w).alias("next_budget"),
        F.last("location").over(w2).alias("last_location"),
    )
    .orderBy("last_location")
)
