from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_name").orderBy("budget")
w2 = Window.partitionBy("location").orderBy("budget")

result = (
    df_departments
    .filter(~F.col("dept_name").isin(["Legal", "Sales"]))
    .select(
        "id",
        "dept_name",
        "budget",
        "location",
        F.rank().over(w).alias("budget_rank"),
        F.rank().over(w2).alias("budget_rank_2"),
    )
)
