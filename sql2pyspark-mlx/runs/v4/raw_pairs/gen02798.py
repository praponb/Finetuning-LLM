from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("manager_id", "level").orderBy("hire_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("job_title")
w3 = Window.orderBy("salary_share", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("job_title").orderBy(F.col("cumulative_salary").asc_nulls_last())
w5 = Window.partitionBy("manager_id").orderBy(F.col("salary").desc())
w6 = Window.partitionBy("job_title", "manager_id").orderBy("hire_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

with_windows = (
    df_employees
    .withColumn("cumulative_salary", F.sum("salary").over(w))
    .withColumn("salary_share", F.round((F.col("salary") / F.sum("salary").over(w2)), 4))
)

filtered = (
    with_windows
    .filter((F.col("hire_date") > F.lit("2023-08-11").cast("date")) & F.col("level").isNotNull())
    .select("*", F.date_format("hire_date", "yyyy-MM-dd").alias("hire_date_label"))
)

metrics = (
    filtered
    .select(
        "*",
        F.sum("salary_share").over(w3).alias("cumulative_salary_share"),
        F.rank().over(w4).alias("cumulative_salary_rank"),
        F.percent_rank().over(w5).alias("salary_percent_rank"),
        F.sum("salary_share").over(w6).alias("cumulative_salary_share_2"),
    )
)

result = (
    metrics
    .withColumn("salary_percent_rank_tier", F.when(F.col("salary_percent_rank") >= 0.73, "LARGE").otherwise("SMALL"))
    .withColumn("week_start", F.date_trunc("WEEK", "hire_date"))
    .groupingSets(
        [["salary_percent_rank_tier", "week_start", "level"], ["salary_percent_rank_tier", "week_start"]],
        "salary_percent_rank_tier",
        "week_start",
        "level",
    )
    .agg(
        F.count(F.when(F.col("level") == "L4", 1)).alias("l4_count"),
        F.size(F.collect_set("job_title")).alias("num_job_titles"),
        F.count(F.when(F.col("level") == "L1", 1)).alias("l1_rows"),
        F.sum(F.when(F.col("job_title") == "Intern", F.col("salary_percent_rank")).otherwise(0)).alias("intern_salary_percent_rank"),
        F.percentile_approx("cumulative_salary_rank", [0.25, 0.5, 0.75]).alias("cumulative_salary_rank_quartiles"),
    )
)
