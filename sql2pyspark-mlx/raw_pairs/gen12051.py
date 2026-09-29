from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.orderBy(F.col("budget").asc_nulls_first())
w2 = Window.partitionBy("location").orderBy(F.col("budget").desc_nulls_last())
w3 = Window.partitionBy("dept_name", "location").orderBy("budget", "id")
w4 = Window.orderBy("budget", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_departments
    .filter(F.col("budget").between(1110000, 1872000))
    .select(
        "id",
        "dept_name",
        F.rank().over(w_recent).alias("budget_rank"),
        F.percent_rank().over(w2).alias("budget_percent_rank"),
        F.row_number().over(w3).alias("seq_no"),
        F.last("dept_name", ignorenulls=True).over(w4).alias("last_known_dept_name"),
    )
)
