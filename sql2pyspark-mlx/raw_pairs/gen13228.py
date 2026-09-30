from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("manager_id", "level").orderBy("salary", "id").rowsBetween(-2, Window.currentRow)
w2 = Window.partitionBy(F.date_trunc("MONTH", "hire_date"), "dept_id").orderBy(F.col("salary").desc())
w3 = Window.orderBy("salary_cume_dist", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_part = Window.orderBy("last3_sum_salary")
w5 = Window.partitionBy("dept_id").orderBy("salary", "id").rowsBetween(-6, Window.currentRow)
w6 = Window.partitionBy("dept_id").orderBy("last3_max_salary", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy("job_title").orderBy(F.col("last3_count_salary").desc_nulls_last())
w8 = Window.partitionBy("manager_id").orderBy("last3_max_salary_avg_6", "id")
w9 = Window.partitionBy("job_title")
w10 = Window.partitionBy("dept_id", "job_title")
w11 = Window.partitionBy("dept_id", "job_title").orderBy("dept_id_count_last3_count_salary", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w12 = Window.partitionBy("dept_id", "job_title").orderBy("last3_sum_salary", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w13 = Window.partitionBy("manager_id", "job_title")
w14 = Window.orderBy("running_last3_avg_salary", "id").rowsBetween(-3, -1)
w15 = Window.partitionBy("dept_id").orderBy("last3_sum_salary", "id").rowsBetween(-6, Window.currentRow)

cleaned = (
    df_employees
    .filter(F.col("manager_id").isNotNull() & F.col("dept_id").isin([9, 14, 25]))
    .select(
        "*",
        F.when(F.col("is_remote"), "Yes").otherwise("No").alias("is_remote_label"),
        F.coalesce("job_title", F.lit("UNKNOWN")).alias("job_title_filled"),
    )
)

with_windows = (
    cleaned
    .filter(F.col("job_title").isNull())
    .select(
        "id",
        "full_name",
        "email",
        "dept_id",
        "manager_id",
        "job_title",
        "salary",
        F.count("salary").over(w).alias("last3_count_salary"),
        F.avg("salary").over(w).alias("last3_avg_salary"),
        F.sum("salary").over(w).alias("last3_sum_salary"),
        F.max("salary").over(w).alias("last3_max_salary"),
        F.cume_dist().over(w2).alias("salary_cume_dist"),
    )
)

enriched = (
    with_windows
    .withColumn("running_last3_avg_salary", F.sum("last3_avg_salary").over(w3))
    .withColumn("last3_sum_salary_dense_rank", F.dense_rank().over(w_part))
    .withColumn("last3_max_salary_avg_6", F.avg("last3_max_salary").over(w5))
)

with_windows_employees = (
    enriched
    .select(
        "*",
        F.last("last3_sum_salary_dense_rank", ignorenulls=True).over(w6).alias("last_known_last3_sum_salary_dense_rank"),
        F.percent_rank().over(w7).alias("last3_count_salary_percent_rank"),
        F.lag("last3_max_salary", 1).over(w8).alias("prev_last3_max_salary"),
        F.min("last3_count_salary").over(w9).alias("job_title_min_last3_count_salary"),
        F.count("last3_count_salary").over(w10).alias("dept_id_count_last3_count_salary"),
    )
)

with_windows_employees_2 = (
    with_windows_employees
    .select(
        "id",
        "dept_id",
        "salary",
        "last3_avg_salary",
        "last3_sum_salary",
        "last3_max_salary",
        "salary_cume_dist",
        "last3_sum_salary_dense_rank",
        "last_known_last3_sum_salary_dense_rank",
        "last3_count_salary_percent_rank",
        "prev_last3_max_salary",
        "dept_id_count_last3_count_salary",
        F.last("last3_avg_salary", ignorenulls=True).over(w11).alias("last_known_last3_avg_salary"),
        F.first("last3_count_salary", ignorenulls=True).over(w12).alias("first_known_last3_count_salary"),
        F.count("salary").over(w13).alias("manager_id_count_salary"),
        F.avg("last3_sum_salary").over(w14).alias("last3_sum_salary_avg_3_prior"),
    )
)

enriched_employees = (
    with_windows_employees_2
    .alias("e")
    .join(df_departments.alias("d"), F.col("e.dept_id") == F.col("d.id"), "inner")
    .select("e.*", F.col("d.budget"))
)

enriched_employees_2 = (
    enriched_employees
    .withColumn("last7_count_last3_avg_salary", F.count("last3_avg_salary").over(w15))
    .withColumn("last7_min_last3_avg_salary", F.min("last3_avg_salary").over(w15))
    .withColumn("last7_sum_last3_avg_salary", F.sum("last3_avg_salary").over(w15))
)

result = (
    enriched_employees_2
    .alias("src")
    .join(df_departments.alias("d"), F.col("src.dept_id") == F.col("d.id"), "inner")
    .rollup(F.col("d.location").alias("location"))
    .agg(
        F.count("*").alias("cnt"),
        F.round(F.sum(F.col("src.last3_sum_salary_dense_rank")), 2).alias("total_last3_sum_salary_dense_rank"),
    )
)
