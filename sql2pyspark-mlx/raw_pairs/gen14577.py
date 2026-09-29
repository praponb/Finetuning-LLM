from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("location").orderBy("budget", "id")
w2 = Window.partitionBy("location").orderBy(F.col("budget").desc_nulls_first())

valid = (
    df_departments
    .filter(F.col("location").isNotNull())
    .withColumn("location_dept_name", F.concat("location", F.lit(" "), "dept_name"))
)

result = (
    valid
    .select(
        "id",
        "dept_name",
        "budget",
        F.lead("budget").over(w).alias("next_budget"),
        F.lag("budget").over(w).alias("prev_budget"),
        F.row_number().over(w).alias("row_seq"),
        F.rank().over(w2).alias("budget_rank"),
    )
)
