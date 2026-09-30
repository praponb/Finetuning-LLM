from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("location").orderBy(F.col("budget").desc(), "id")
w2 = Window.partitionBy("dept_name").orderBy("budget", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

scored = (
    df_departments
    .select(
        "*",
        F.row_number().over(w).alias("row_seq"),
        F.first("budget", ignorenulls=True).over(w2).alias("first_known_budget"),
    )
)

result = (
    scored
    .rollup("location")
    .agg(
        F.sum(F.when(F.col("dept_name") == "HR", F.col("first_known_budget")).otherwise(0)).alias("hr_first_known_budget"),
        F.count(F.when(F.col("location") == "Chiang Mai", 1)).alias("chiang_mai_count"),
        F.grouping("location").alias("gid"),
    )
    .filter(F.col("hr_first_known_budget") < 1000000)
)
