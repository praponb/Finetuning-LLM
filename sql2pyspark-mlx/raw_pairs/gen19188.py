from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.salary").desc(), F.col("c.id").desc())

latest_match = (
    df_departments
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.job_title"), F.col("c.salary"))
)

result = (
    latest_match
    .groupBy(
        F.when(F.col("budget") >= 3060000, "LARGE").otherwise("SMALL").alias("budget_tier"),
    )
    .agg(
        F.sum(F.when(F.col("dept_name") == "Legal", F.col("budget")).otherwise(0)).alias("legal_budget"),
        F.count(F.when(F.col("dept_name") == "Marketing", 1)).alias("marketing_rows"),
        F.percentile_approx("salary", 0.95).alias("salary_p95"),
        F.count(F.when(F.col("dept_name") == "Legal", 1)).alias("legal_count"),
    )
    .orderBy(F.col("legal_count").desc())
)
