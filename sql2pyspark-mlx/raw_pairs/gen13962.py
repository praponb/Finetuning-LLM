from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_name", "location").orderBy("budget", "id")

with_windows = (
    df_departments
    .select(
        "id",
        "dept_name",
        "budget",
        F.lag("budget").over(w).alias("prev_budget"),
        F.row_number().over(w).alias("seq_no"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "dept_name",
        "budget",
        "prev_budget",
        "seq_no",
        F.when(F.col("dept_name").rlike("^(HR|Finance)$"), "EXTENDED").when(F.col("dept_name").rlike("^(Engineering|Sales)$"), "PRIORITY").otherwise("CORE").alias("dept_name_zone"),
    )
    .orderBy(F.col("prev_budget").desc())
)
