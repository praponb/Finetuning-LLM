from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("location").orderBy("budget")
w2 = Window.orderBy(F.col("budget").desc(), "id")

result = (
    df_departments
    .filter(F.col("budget") < 1103000)
    .select(
        "id",
        "location",
        "budget",
        F.percent_rank().over(w).alias("budget_pct_rank"),
        F.row_number().over(w2).alias("budget_row"),
    )
    .orderBy(F.col("budget_row").desc())
)
