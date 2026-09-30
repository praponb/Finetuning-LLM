from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("manager_id").orderBy("hire_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("dept_id").orderBy("salary", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("job_title").orderBy("hire_date", "id")
w4 = Window.partitionBy("level").orderBy("salary")
w5 = Window.orderBy("salary", "id").rowsBetween(-2, Window.currentRow)
w6 = Window.partitionBy("job_title")
w7 = Window.partitionBy("job_title").orderBy(F.col("salary").desc())
w8 = Window.partitionBy("job_title").orderBy("cumulative_salary", "id").rowsBetween(-6, -1)

valid = (
    df_employees
    .filter((F.col("hire_date") > F.lit("2023-04-30").cast("date")) & F.col("job_title").isNotNull())
    .select(
        "*",
        F.when(F.col("is_remote"), "Yes").otherwise("No").alias("is_remote_label"),
    )
)

scored = (
    valid
    .withColumn("first_known_job_title", F.first("job_title", ignorenulls=True).over(w))
    .withColumn("cumulative_salary", F.sum("salary").over(w2))
    .withColumn("prev_salary", F.lag("salary", 2).over(w3))
    .withColumn("salary_dense_rank", F.dense_rank().over(w4))
    .withColumn("salary_avg_2", F.avg("salary").over(w5))
)

with_windows = (
    scored
    .withColumn("salary_dense_rank_share", F.round((F.col("salary_dense_rank") / F.sum("salary_dense_rank").over(w6)), 4))
    .withColumn("salary_cume_dist", F.cume_dist().over(w7))
    .withColumn("salary_avg_6_prior", F.avg("salary").over(w8))
)

result = (
    with_windows
    .groupBy(F.date_trunc("MONTH", "hire_date").alias("activity_month"))
    .agg(
        F.round(F.avg("salary_avg_2"), 2).alias("avg_salary_avg_2"),
        F.count(F.when(F.col("level") == "L4", 1)).alias("l4_rows"),
        F.size(F.collect_set("job_title")).alias("num_job_titles"),
    )
    .orderBy(F.col("l4_rows").desc())
)
