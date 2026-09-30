from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_name", "location").orderBy("budget", "id")

result = (
    df_departments
    .filter(F.col("location").isin(["Chiang Mai", "Tokyo", "London"]))
    .select(
        "id",
        "dept_name",
        "location",
        "budget",
        (F.col("budget") - F.lag("budget", 1).over(w)).alias("budget_change"),
    )
)
