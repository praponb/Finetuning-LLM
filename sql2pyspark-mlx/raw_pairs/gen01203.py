from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_name").orderBy("budget", "id").rowsBetween(-3, -1)
w2 = Window.orderBy("budget", "id").rowsBetween(-6, Window.currentRow)
w3 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.salary").desc(), F.col("c.id").desc())
w4 = Window.orderBy(F.col("budget_avg_6").desc_nulls_first())
w5 = Window.partitionBy("employee_job_title")

base = (
    df_departments
    .filter(F.col("budget").between(1274000, 2613000))
    .select("*", F.upper("dept_name").alias("dept_name_upper"))
)

scored = (
    base
    .filter(F.col("location").isin(["Tokyo", "Chiang Mai", "Berlin"]))
    .select(
        "id",
        "location",
        "budget",
        "dept_name_upper",
        F.avg("budget").over(w).alias("budget_avg_3_prior"),
        F.avg("budget").over(w2).alias("budget_avg_6"),
    )
)

with_latest = (
    scored
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.location",
        "s.dept_name_upper",
        "s.budget_avg_6",
        F.col("c.job_title").alias("employee_job_title"),
    )
)

remaining_keys = (
    with_latest
    .select("id", "location")
    .exceptAll(
        df_departments
        .filter(F.col("dept_name").isin(["Sales", "Marketing", "Legal"]))
        .select("id", "location")
    )
)

verified = (
    with_latest
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.location") == F.col("k.location")), "inner")
    .select("s.*")
)

enriched = (
    verified
    .select(
        "id",
        "location",
        "employee_job_title",
        F.dense_rank().over(w4).alias("budget_avg_6_dense_rank"),
    )
)

valid = (
    enriched
    .filter(F.col("location").isNotNull() & (~F.col("employee_job_title").eqNullSafe(F.lit("Intern"))))
    .select(
        "*",
        F.round("budget_avg_6_dense_rank", 2).alias("budget_avg_6_dense_rank_rounded"),
        F.coalesce("employee_job_title", F.lit("Unknown")).alias("employee_job_title_filled"),
    )
)

enriched_departments = (
    valid
    .select(
        "budget_avg_6_dense_rank_rounded",
        "employee_job_title_filled",
        F.sum("budget_avg_6_dense_rank_rounded").over(w5).alias("employee_job_title_sum_budget_avg_6_dense_rank_rounded"),
    )
)

result = (
    enriched_departments
    .groupBy(
        F.when(F.col("employee_job_title_sum_budget_avg_6_dense_rank_rounded") >= 376, "LARGE").otherwise("SMALL").alias("employee_job_title_sum_budget_avg_6_dense_rank_rounded_tier"),
    )
    .agg(
        F.sum("employee_job_title_sum_budget_avg_6_dense_rank_rounded").alias("total_employee_job_title_sum_budget_avg_6_dense_rank_rounded"),
        F.count("*").alias("num_rows"),
    )
    .filter(F.col("num_rows") < 1000)
)
