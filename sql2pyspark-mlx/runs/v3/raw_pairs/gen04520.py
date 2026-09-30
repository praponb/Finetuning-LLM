from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_name").orderBy("budget", "id")

result = (
    df_departments
    .filter(F.col("dept_name").isin(["Sales", "Legal"]))
    .select(
        "id",
        "dept_name",
        "budget",
        (F.col("budget") - F.lag("budget", 1).over(w)).alias("budget_change"),
    )
    .orderBy(F.col("budget").desc(), "dept_name")
)
