from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_name", "location").orderBy("budget", "id").rowsBetween(-2, Window.currentRow)

cleaned = (
    df_departments
    .filter(F.col("budget") > 1578000)
    .select(
        "*",
        F.coalesce("location", F.lit("Unknown")).alias("location_filled"),
        F.coalesce("dept_name", F.lit("DIRECT")).alias("dept_name_filled"),
    )
)

result = (
    cleaned
    .filter(F.col("budget").isNull())
    .select(
        "id",
        "dept_name",
        "location_filled",
        F.max("budget").over(w).alias("last3_max_budget"),
        F.sum("budget").over(w).alias("last3_sum_budget"),
        F.count("budget").over(w).alias("last3_count_budget"),
    )
)
