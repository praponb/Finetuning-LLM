from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_name")
w2 = Window.partitionBy("location").orderBy("budget", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

metrics = (
    df_departments
    .select(
        "*",
        F.min("budget").over(w).alias("dept_name_min_budget"),
        F.first("dept_name", ignorenulls=True).over(w2).alias("first_known_dept_name"),
    )
)

result = (
    metrics
    .select(
        "id",
        "location",
        "budget",
        "dept_name_min_budget",
        "first_known_dept_name",
        F.when(F.col("location").rlike("^(Chiang Mai|Bangkok)$"), "OTHER_ZONE").when(F.col("location").rlike("^(New York|Singapore)$"), "GROUP_B").otherwise("EXTENDED").alias("location_zone"),
    )
    .orderBy(F.col("location").desc())
)
