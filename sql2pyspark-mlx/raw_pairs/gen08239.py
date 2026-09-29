from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("budget").desc(), "id")
w2 = Window.partitionBy("dept_name", "location").orderBy("budget", "id")

with_windows = (
    df_departments
    .withColumn("event_seq", F.row_number().over(w))
    .withColumn("prev_budget", F.lag("budget", 2).over(w2))
)

result = (
    with_windows
    .select(
        "id",
        "dept_name",
        "location",
        "budget",
        F.when(F.col("dept_name").rlike("^(Marketing|Legal)$"), "PRIORITY").otherwise("CORE").alias("dept_name_zone"),
    )
    .orderBy(F.col("location").desc_nulls_first())
)
