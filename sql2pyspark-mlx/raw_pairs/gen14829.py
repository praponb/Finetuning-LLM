from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.salary").desc(), F.col("c.id").desc())

with_last = (
    df_departments
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.level"), F.col("c.hire_date"))
)

result = (
    with_last
    .withColumn("year_start", F.date_trunc("YEAR", "hire_date"))
    .withColumn("budget_tier", F.when(F.col("budget") >= 4382000, "LARGE").otherwise("SMALL"))
    .withColumn("week_start", F.date_trunc("WEEK", "hire_date"))
    .groupingSets(
        [["year_start", "budget_tier", "week_start"], ["year_start", "budget_tier"], ["year_start"]],
        "year_start",
        "budget_tier",
        "week_start",
    )
    .agg(
        F.percentile_approx("budget", 0.5).alias("budget_p50"),
        F.sum(F.when(F.col("location") == "Singapore", F.col("budget")).otherwise(0)).alias("singapore_budget"),
        F.count(F.when(F.col("dept_name") == "Engineering", 1)).alias("engineering_rows"),
        F.max("budget").alias("max_budget"),
    )
    .orderBy(F.col("budget_p50").asc_nulls_first())
)
