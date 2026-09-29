from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_name", "location").orderBy(F.col("budget").desc())

result = (
    df_departments
    .filter(F.col("dept_name").isin(["Marketing", "Engineering", "HR"]))
    .select(
        "id",
        "dept_name",
        "location",
        "budget",
        F.percent_rank().over(w).alias("budget_pct_rank"),
    )
    .orderBy("budget_pct_rank", F.col("budget").desc())
)
