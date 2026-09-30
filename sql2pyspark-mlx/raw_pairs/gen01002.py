from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("location", "dept_name").orderBy("budget", "id").rowsBetween(-6, Window.currentRow)
w2 = Window.partitionBy("location").orderBy("budget", "id")
w3 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.hire_date").desc(), F.col("c.id").desc())
w4 = Window.partitionBy("job_title").orderBy(F.col("last7_min_budget_rounded").desc_nulls_first(), "id")
w5 = Window.partitionBy("job_title").orderBy("last7_min_budget_rounded", "id")

enriched = (
    df_departments
    .select(
        "id",
        "dept_name",
        F.sum("budget").over(w).alias("last7_sum_budget"),
        F.max("budget").over(w).alias("last7_max_budget"),
        F.min("budget").over(w).alias("last7_min_budget"),
        F.lag("budget", 1).over(w2).alias("prev_budget"),
    )
)

valid = (
    enriched
    .filter(F.col("dept_name").eqNullSafe(F.lit("Engineering")))
    .select("*", F.round("last7_min_budget", 1).alias("last7_min_budget_rounded"))
)

latest_match = (
    valid
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
    .select("s.id", "s.dept_name", "s.last7_min_budget_rounded", F.col("c.job_title"))
)

deduped = (
    latest_match
    .withColumn("dedup_rank", F.row_number().over(w4))
    .filter(F.col("dedup_rank") == 1)
    .drop("dedup_rank")
)

ranked = (
    deduped
    .withColumn("rn", F.row_number().over(w5))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

result = (
    ranked
    .select("id", "dept_name", "last7_min_budget_rounded")
    .orderBy(F.col("last7_min_budget_rounded").asc_nulls_first())
)
