from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_name").orderBy(F.col("budget").desc())
w2 = Window.orderBy("budget", "id")

result = (
    df_departments
    .select(
        "id",
        "dept_name",
        "location",
        "budget",
        F.rank().over(w).alias("budget_rank"),
        F.row_number().over(w2).alias("seq_no"),
    )
)
