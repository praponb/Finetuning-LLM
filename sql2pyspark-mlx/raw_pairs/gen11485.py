from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("manager_id", "job_title").orderBy("hire_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy("salary", "id").rowsBetween(-6, Window.currentRow)
w3 = Window.partitionBy("level").orderBy("hire_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy(F.date_trunc("week", "hire_date"), "job_title").orderBy("hire_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("level").orderBy("cumulative_salary_3", "id")
w6 = Window.partitionBy("level").orderBy("cumulative_salary_2", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_part = Window.partitionBy("manager_id", "level").orderBy("salary_avg_6", "id").rowsBetween(-2, Window.currentRow)
w8 = Window.partitionBy("job_title").orderBy("cumulative_salary_2", "id")
w9 = Window.partitionBy("level").orderBy(F.col("last3_sum_cumulative_salary").asc_nulls_first())
w_hist = Window.partitionBy("job_title", "level").orderBy("cumulative_salary_2", "id")
w_hist_2 = Window.partitionBy("level").orderBy("last3_max_cumulative_salary", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w12 = Window.partitionBy("job_title").orderBy(F.col("distinct_ids").desc())

enriched = (
    df_employees
    .select(
        "id",
        "full_name",
        "email",
        "manager_id",
        "job_title",
        "hire_date",
        "level",
        F.sum("salary").over(w).alias("cumulative_salary"),
        F.avg("salary").over(w2).alias("salary_avg_6"),
        F.sum("salary").over(w3).alias("cumulative_salary_2"),
        F.sum("salary").over(w4).alias("cumulative_salary_3"),
    )
)

enriched_employees = (
    enriched
    .withColumn("next_cumulative_salary_2", F.lead("cumulative_salary_2", 2, 0).over(w5))
    .withColumn("last_known_job_title", F.last("job_title", ignorenulls=True).over(w6))
)

enriched_employees_2 = (
    enriched_employees
    .select(
        "id",
        "email",
        "job_title",
        "level",
        "cumulative_salary",
        "cumulative_salary_2",
        "last_known_job_title",
        F.sum("cumulative_salary").over(w_part).alias("last3_sum_cumulative_salary"),
        F.min("cumulative_salary").over(w_part).alias("last3_min_cumulative_salary"),
        F.max("cumulative_salary").over(w_part).alias("last3_max_cumulative_salary"),
    )
)

with_windows = (
    enriched_employees_2
    .withColumn("seq_no", F.row_number().over(w8))
    .withColumn("last3_sum_cumulative_salary_dense_rank", F.dense_rank().over(w9))
)

scored = (
    with_windows
    .withColumn("next_cumulative_salary_2", F.lead("cumulative_salary_2").over(w_hist))
    .withColumn("prev_last3_sum_cumulative_salary_dense_rank", F.lag("last3_sum_cumulative_salary_dense_rank").over(w_hist))
    .withColumn("row_seq", F.row_number().over(w_hist))
    .withColumn("cumulative_cumulative_salary", F.sum("cumulative_salary").over(w_hist_2))
)

result = (
    scored
    .groupBy("job_title", "level")
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.percentile_approx("row_seq", 0.9).alias("row_seq_p90"),
        F.count_if(F.col("level") == "L4").alias("l4_events"),
        F.min("last3_sum_cumulative_salary").alias("min_last3_sum_cumulative_salary"),
        F.countDistinct("id").alias("distinct_ids_2"),
    )
    .withColumn("distinct_ids_dense_rank", F.dense_rank().over(w12))
)
