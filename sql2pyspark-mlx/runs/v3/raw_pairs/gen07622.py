from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_name")
w2 = Window.partitionBy("dept_name").orderBy("budget")

result = (
    df_departments
    .select(
        "id",
        "dept_name",
        "budget",
        F.round((F.col("budget") / F.sum("budget").over(w)), 3).alias("budget_share"),
        F.rank().over(w2).alias("budget_rank"),
    )
)
