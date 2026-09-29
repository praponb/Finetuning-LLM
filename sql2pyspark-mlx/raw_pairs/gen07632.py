from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_id", "job_title").orderBy("salary", "id")
w_running = Window.partitionBy("job_title").orderBy("salary", "id").rowsBetween(-2, Window.currentRow)
w3 = Window.partitionBy("dept_id").orderBy("salary")
w_hist = Window.partitionBy("job_title").orderBy("last3_max_salary", "id").rowsBetween(-3, Window.currentRow)
w5 = Window.partitionBy("job_title", "dept_id").orderBy(F.col("last4_avg_salary").desc(), "id")

filtered = (
    df_employees
    .filter(F.col("manager_id").isin([3, 12, 25]))
    .select(
        "id",
        "full_name",
        "email",
        "dept_id",
        "manager_id",
        "job_title",
        "salary",
        F.trim("full_name").alias("clean_full_name"),
    )
)

deduped = (
    filtered
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

valid = (
    deduped
    .filter(F.col("dept_id").isin([15, 22, 23, 26]))
    .withColumn("full_name_upper", F.upper("full_name"))
    .withColumn("job_title_clean", F.coalesce("job_title", F.lit("DIRECT")))
)

joined = (
    valid
    .alias("a")
    .join(df_departments.alias("p"), F.col("a.dept_id") == F.col("p.id"), "left")
    .select(
        "a.id",
        "a.email",
        "a.dept_id",
        "a.job_title",
        "a.salary",
        "a.full_name_upper",
        "a.job_title_clean",
        F.col("p.dept_name"),
    )
)

enriched = (
    joined
    .filter(F.lower("email").like("%vy@%"))
    .select(
        "*",
        F.count("salary").over(w_running).alias("last3_count_salary"),
        F.min("salary").over(w_running).alias("last3_min_salary"),
        F.max("salary").over(w_running).alias("last3_max_salary"),
        F.rank().over(w3).alias("salary_rank"),
    )
)

enriched_employees = (
    enriched
    .select(
        "id",
        "email",
        "dept_id",
        "job_title",
        "full_name_upper",
        "dept_name",
        "last3_count_salary",
        "last3_min_salary",
        "last3_max_salary",
        F.max("salary").over(w_hist).alias("last4_max_salary"),
        F.count("salary").over(w_hist).alias("last4_count_salary"),
        F.avg("salary").over(w_hist).alias("last4_avg_salary"),
        F.sum("salary").over(w_hist).alias("last4_sum_salary"),
    )
)

latest = (
    enriched_employees
    .withColumn("row_num", F.row_number().over(w5))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

result = (
    latest
    .alias("src")
    .join(df_departments.alias("dim"), F.col("src.dept_id") == F.col("dim.id"), "inner")
    .rollup(
        F.col("dim.dept_name").alias("department_dept_name"),
        F.col("src.job_title").alias("job_title"),
    )
    .agg(
        F.count("*").alias("cnt"),
        F.max(F.col("src.last4_sum_salary")).alias("max_last4_sum_salary"),
        F.avg(F.col("src.last4_avg_salary")).alias("avg_last4_avg_salary"),
        F.grouping_id().alias("level_id"),
    )
)
