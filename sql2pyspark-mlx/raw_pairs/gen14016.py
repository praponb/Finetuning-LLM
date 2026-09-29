from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level").orderBy(F.col("salary").desc(), "id")
w2 = Window.partitionBy("level")
w3 = Window.partitionBy("manager_id").orderBy("salary", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("job_title")
w5 = Window.partitionBy(F.date_trunc("WEEK", "employee_hire_date"), "manager_id").orderBy("salary", "id")
w_recent = Window.partitionBy("job_title").orderBy(F.col("salary_bucket").desc(), "id")
w7 = Window.partitionBy("manager_id", "level").orderBy(F.col("salary_bucket_bucket").desc(), "id")

enriched = (
    df_employees
    .alias("s")
    .join(df_employees.alias("p"), F.col("s.manager_id") == F.col("p.id"), "left")
    .select("s.*", F.col("p.hire_date").alias("employee_hire_date"))
)

metrics = (
    enriched
    .withColumn("salary_bucket", F.ntile(3).over(w))
    .withColumn("salary_share", F.round((F.col("salary") / F.sum("salary").over(w2)), 4))
    .withColumn("last_known_level", F.last("level", ignorenulls=True).over(w3))
)

metrics_employees = (
    metrics
    .withColumn("job_title_avg_salary_share", F.avg("salary_share").over(w4))
    .withColumn("row_seq", F.row_number().over(w5))
    .withColumn("salary_bucket_bucket", F.ntile(10).over(w_recent))
)

deduped = (
    metrics_employees
    .withColumn("dedup_rank", F.row_number().over(w7))
    .filter(F.col("dedup_rank") == 1)
    .drop("dedup_rank")
)

result = (
    deduped
    .groupBy(F.date_trunc("QUARTER", "employee_hire_date").alias("quarter_start"))
    .agg(
        F.count("*").alias("num_rows"),
        F.sum(F.when(F.col("level") == "L5", F.col("job_title_avg_salary_share"))).alias("l5_total_job_title_avg_salary_share"),
    )
)
