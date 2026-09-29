from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level").orderBy("salary_whole", "id").rowsBetween(-5, Window.currentRow)
w2 = Window.partitionBy("manager_id", "dept_id").orderBy("days_since_hire_date", "id").rowsBetween(-6, Window.currentRow)
w3 = Window.orderBy("salary_whole", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("level").orderBy("salary_whole_avg_5", "id").rowsBetween(-2, Window.currentRow)
w5 = Window.partitionBy("level").orderBy("days_since_hire_date", "id")
w_running = Window.partitionBy("level").orderBy("days_since_hire_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy("level", "manager_id").orderBy("days_since_hire_date_bucket", "id").rowsBetween(-6, -1)
w8 = Window.partitionBy("level", "manager_id").orderBy(F.col("days_since_hire_date_bucket").desc())
w9 = Window.partitionBy("level").orderBy("days_since_hire_date_bucket", "id").rowsBetween(-6, Window.currentRow)
w10 = Window.orderBy(F.col("last_known_days_since_hire_date").desc())

base = (
    df_employees
    .filter(F.col("job_title").isNotNull())
    .select(
        "id",
        "full_name",
        "email",
        "dept_id",
        "manager_id",
        "level",
        F.datediff(F.current_date(), "hire_date").alias("days_since_hire_date"),
        F.ceil("salary").alias("salary_whole"),
        F.coalesce("level", F.lit("OTHER")).alias("level_clean"),
    )
)

scored = (
    base
    .filter(F.col("salary_whole") > 50)
    .withColumn("salary_whole_avg_5", F.avg("salary_whole").over(w))
    .withColumn("salary_whole_avg_6", F.avg("salary_whole").over(w2))
    .withColumn("last_known_days_since_hire_date", F.last("days_since_hire_date", ignorenulls=True).over(w3))
)

scored_employees = (
    scored
    .select(
        "id",
        "full_name",
        "email",
        "manager_id",
        "level",
        "days_since_hire_date",
        "salary_whole",
        "level_clean",
        "last_known_days_since_hire_date",
        F.avg("salary_whole_avg_6").over(w4).alias("salary_whole_avg_6_avg_2"),
        F.ntile(5).over(w5).alias("days_since_hire_date_bucket"),
    )
)

enriched = (
    scored_employees
    .select(
        "*",
        F.sum("salary_whole_avg_6_avg_2").over(w_running).alias("cumulative_salary_whole_avg_6_avg_2"),
        F.avg("salary_whole_avg_6_avg_2").over(w7).alias("salary_whole_avg_6_avg_2_avg_6_prior"),
        F.percent_rank().over(w8).alias("days_since_hire_date_bucket_percent_rank"),
        F.avg("days_since_hire_date_bucket").over(w9).alias("days_since_hire_date_bucket_avg_6"),
        F.rank().over(w10).alias("last_known_days_since_hire_date_rank"),
    )
)

totals = (
    enriched
    .groupBy("manager_id")
    .agg(
        F.count_if(F.col("level") == "L1").alias("l1_events"),
        F.round(F.avg("days_since_hire_date_bucket_percent_rank"), 2).alias("avg_days_since_hire_date_bucket_percent_rank"),
        F.round(F.avg("days_since_hire_date_bucket"), 2).alias("avg_days_since_hire_date_bucket"),
        F.count(F.when(F.col("level") == "L4", 1)).alias("l4_rows"),
        F.avg("salary_whole_avg_6_avg_2").alias("avg_salary_whole_avg_6_avg_2"),
    )
)

result = (
    totals
    .select(
        "manager_id",
        "l1_events",
        "avg_days_since_hire_date_bucket_percent_rank",
        "avg_salary_whole_avg_6_avg_2",
    )
    .orderBy(F.col("avg_days_since_hire_date_bucket_percent_rank").desc_nulls_last())
)
