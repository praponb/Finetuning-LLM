from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("budget", "id")
w2 = Window.orderBy(F.col("budget").desc())

result = (
    df_departments
    .filter(F.col("dept_name").isin(["Legal", "HR", "Sales"]))
    .select(
        "id",
        "budget",
        F.row_number().over(w).alias("budget_row"),
        F.dense_rank().over(w2).alias("dense_rnk"),
    )
)
