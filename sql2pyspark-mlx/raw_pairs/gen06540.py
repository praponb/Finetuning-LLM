from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_name").orderBy(F.col("budget").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

result = (
    df_departments
    .filter(F.col("location").isNotNull())
    .select(
        "id",
        "dept_name",
        "budget",
        "location",
        F.last("location").over(w).alias("last_location"),
    )
    .orderBy(F.col("budget").desc())
)
