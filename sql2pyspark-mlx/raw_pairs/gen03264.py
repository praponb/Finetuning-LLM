from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("budget").desc(), "id")
w2 = Window.partitionBy("location").orderBy(F.col("budget").desc())
w_part = Window.partitionBy("dept_name", "location").orderBy("budget", "id")
w4 = Window.partitionBy("location").orderBy("budget", "id")

scored = (
    df_departments
    .select(
        "*",
        F.ntile(3).over(w).alias("budget_bucket"),
        F.percent_rank().over(w2).alias("budget_percent_rank"),
        F.lag("budget", 2).over(w_part).alias("prev_budget"),
        F.ntile(3).over(w4).alias("budget_bucket_2"),
    )
)

result = (
    scored
    .select(
        "id",
        "location",
        "budget",
        "budget_bucket",
        "budget_percent_rank",
        "budget_bucket_2",
        F.when(F.col("location").rlike("^(New York|Berlin)$"), "GROUP_B").when(F.col("location").rlike("^(Bangkok|Chiang Mai)$"), "GROUP_A").otherwise("CORE").alias("location_zone"),
    )
)
