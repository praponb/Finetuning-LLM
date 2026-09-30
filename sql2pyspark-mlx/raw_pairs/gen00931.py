from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("manager_id").orderBy(F.col("salary").desc())
w2 = Window.partitionBy("dept_id", "job_title").orderBy("hire_date", "id")
w3 = Window.partitionBy("job_title").orderBy("hire_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("dept_id", "manager_id").orderBy(F.col("prev_salary").asc_nulls_last(), "id")
w_ordered = Window.partitionBy("dept_id", "manager_id").orderBy("salary_dense_rank", "id").rowsBetween(-3, Window.currentRow)
w6 = Window.partitionBy("manager_id").orderBy("salary_dense_rank", "id")

filtered = (
    df_employees
    .filter(F.col("job_title").isNotNull())
    .select(
        "*",
        F.concat_ws(",", "job_title", "email").alias("job_title_email_key"),
        F.coalesce("level", F.lit("OTHER")).alias("resolved_level"),
    )
)

enriched = (
    filtered
    .filter(F.upper("full_name").like("%ENR%"))
    .select(
        "id",
        "full_name",
        "email",
        "dept_id",
        "manager_id",
        "hire_date",
        "resolved_level",
        F.dense_rank().over(w).alias("salary_dense_rank"),
        F.lag("salary", 1, 0).over(w2).alias("prev_salary"),
        F.first("level", ignorenulls=True).over(w3).alias("first_known_level"),
    )
)

latest = (
    enriched
    .withColumn("dedup_rank", F.row_number().over(w4))
    .filter(F.col("dedup_rank") == 1)
)

base = (
    latest
    .filter(F.col("hire_date") <= "2024-07-01")
    .select("*", F.initcap(F.trim("full_name")).alias("full_name_title"))
)

result = (
    base
    .select(
        "*",
        F.avg("prev_salary").over(w_ordered).alias("last4_avg_prev_salary"),
        F.max("prev_salary").over(w_ordered).alias("last4_max_prev_salary"),
        F.sum("prev_salary").over(w_ordered).alias("last4_sum_prev_salary"),
        F.count("prev_salary").over(w_ordered).alias("last4_count_prev_salary"),
        F.ntile(5).over(w6).alias("salary_dense_rank_bucket"),
    )
)
