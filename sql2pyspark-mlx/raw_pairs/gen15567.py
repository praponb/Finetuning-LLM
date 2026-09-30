from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("budget").desc(), "id")
w2 = Window.orderBy("budget", "id").rowsBetween(-3, -1)
w3 = Window.partitionBy("location").orderBy("budget", "id")
w4 = Window.partitionBy("location").orderBy("budget", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_grp = Window.partitionBy("location").orderBy(F.col("budget").desc_nulls_first())

result = (
    df_departments
    .select(
        "id",
        "budget",
        F.row_number().over(w).alias("seq_no"),
        F.avg("budget").over(w2).alias("budget_avg_3_prior"),
        F.row_number().over(w3).alias("row_seq"),
        F.sum("budget").over(w4).alias("running_budget"),
        F.cume_dist().over(w_grp).alias("budget_cume_dist"),
    )
)
