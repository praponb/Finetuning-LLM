from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.hire_date").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("budget_tier").orderBy(F.col("hr_budget").desc())

with_latest = (
    df_departments
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.hire_date"))
)

result = (
    with_latest
    .groupBy(
        F.when(F.col("budget") >= 3983000, "PREMIUM").otherwise("STANDARD").alias("budget_tier"),
        F.date_trunc("WEEK", "hire_date").alias("week_start"),
    )
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.sum(F.when(F.col("dept_name") == "HR", F.col("budget")).otherwise(0)).alias("hr_budget"),
    )
    .withColumn("hr_budget_rank", F.rank().over(w2))
    .orderBy(F.col("week_start").asc_nulls_last())
)
