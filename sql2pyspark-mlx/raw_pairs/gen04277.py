from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("manager_id", "level").orderBy("hire_date", "id")
w2 = Window.partitionBy(F.year("hire_date"))
w_running = Window.partitionBy("manager_id").orderBy("salary", "id").rowsBetween(-3, Window.currentRow)
w4 = Window.partitionBy(F.date_trunc("month", "hire_date")).orderBy("seq_no", "id")
w5 = Window.partitionBy("level", "employee_level").orderBy("last4_count_salary_share", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.orderBy(F.col("last4_avg_salary_share").desc(), "id")
w7 = Window.orderBy(F.col("last4_count_salary_share").desc(), "id")
w8 = Window.partitionBy("employee_level").orderBy(F.col("last4_count_salary_share_bucket").desc())

valid = (
    df_employees
    .filter(F.col("hire_date") >= F.lit("2023-08-29").cast("date"))
    .withColumn("job_title_prefix", F.substring("job_title", 1, 3))
    .withColumn("level_full_name_key", F.concat_ws("/", "level", "full_name"))
    .withColumn("level_clean", F.coalesce("level", F.lit("N/A")))
)

enriched = (
    valid
    .filter(F.col("job_title") != "Director")
    .select(
        "*",
        F.lag("salary").over(w).alias("prev_salary"),
        F.lead("salary").over(w).alias("next_salary"),
        F.row_number().over(w).alias("seq_no"),
        F.round((F.col("salary") / F.sum("salary").over(w2)), 4).alias("salary_share"),
    )
)

metrics = (
    enriched
    .select(
        "id",
        "full_name",
        "email",
        "manager_id",
        "salary",
        "hire_date",
        "is_remote",
        "level",
        "next_salary",
        "salary_share",
        F.max("salary_share").over(w_running).alias("last4_max_salary_share"),
        F.count("salary_share").over(w_running).alias("last4_count_salary_share"),
        F.avg("salary_share").over(w_running).alias("last4_avg_salary_share"),
        F.ntile(4).over(w4).alias("seq_no_bucket"),
    )
)

with_dims = (
    metrics
    .alias("a")
    .join(df_employees.alias("p"), F.col("a.manager_id") == F.col("p.id"), "left")
    .select(
        "a.id",
        "a.email",
        "a.manager_id",
        "a.hire_date",
        "a.is_remote",
        "a.level",
        "a.next_salary",
        "a.salary_share",
        "a.last4_max_salary_share",
        "a.last4_count_salary_share",
        "a.last4_avg_salary_share",
        F.col("p.level").alias("employee_level"),
        F.col("p.full_name").alias("employee_full_name"),
        F.col("p.hire_date").alias("employee_hire_date"),
    )
)

with_windows = (
    with_dims
    .select(
        "*",
        F.sum("next_salary").over(w5).alias("running_next_salary"),
        F.ntile(3).over(w6).alias("last4_avg_salary_share_bucket"),
        F.ntile(4).over(w7).alias("last4_count_salary_share_bucket"),
    )
)

ranked = (
    with_windows
    .withColumn("rank_in_group", F.rank().over(w8))
    .filter(F.col("rank_in_group") == 1)
    .drop("rank_in_group")
)

result = (
    ranked
    .groupBy("employee_level", "manager_id")
    .pivot("level", ["L3", "L4"])
    .agg(F.avg("running_next_salary").alias("total"), F.count(F.lit(1)).alias("cnt"))
)
