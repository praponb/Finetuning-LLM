from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level").orderBy(F.col("hire_date").asc_nulls_first(), "id")
w2 = Window.partitionBy("location").orderBy(F.col("salary").desc())
w3 = Window.orderBy("salary", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("dept_id").orderBy("budget", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("job_title", "location").orderBy("hire_date", "id").rowsBetween(-5, -1)
w6 = Window.partitionBy("location")

base = (
    df_employees
    .filter(F.col("hire_date") > F.lit("2023-04-08").cast("date"))
    .withColumn("days_since_hire_date", F.datediff(F.current_date(), "hire_date"))
)

joined = (
    base
    .alias("s")
    .join(df_departments.alias("d"), F.col("s.dept_id") == F.col("d.id"), "inner")
    .select("s.*", F.col("d.dept_name"), F.col("d.budget"), F.col("d.location"))
)

ranked = (
    joined
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

result = (
    ranked
    .withColumn("salary_rank", F.rank().over(w2))
    .withColumn("cumulative_budget", F.sum("budget").over(w3))
    .withColumn("last_known_location", F.last("location", ignorenulls=True).over(w4))
    .withColumn("budget_avg_5_prior", F.avg("budget").over(w5))
    .withColumn("budget_share", F.round((F.col("budget") / F.sum("budget").over(w6)), 4))
)
