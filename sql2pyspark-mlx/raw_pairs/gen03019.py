from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_name").orderBy("budget", "id")
w2 = Window.partitionBy("location").orderBy("budget", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_departments
    .filter(F.col("dept_name") == "Marketing")
    .select(
        "id",
        "dept_name",
        "location",
        "budget",
        F.lead("budget", 2).over(w).alias("next_budget"),
        F.first("dept_name", ignorenulls=True).over(w2).alias("first_known_dept_name"),
    )
)
