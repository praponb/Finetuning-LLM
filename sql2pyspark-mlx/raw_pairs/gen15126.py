from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level", "job_title").orderBy(F.col("salary").desc(), "id")
w2 = Window.partitionBy("level", "job_title").orderBy("salary", "id")
w3 = Window.partitionBy(F.month("hire_date"), "level").orderBy(F.col("salary").desc(), "id")
w4 = Window.partitionBy("job_title")
w5 = Window.partitionBy("level").orderBy("row_num", "id").rowsBetween(-3, Window.currentRow)
w6 = Window.orderBy("salary_bucket", "id")
w7 = Window.partitionBy(F.date_trunc("MONTH", "hire_date"))
w8 = Window.partitionBy("level")
w9 = Window.partitionBy("level").orderBy(F.col("hire_date").desc(), "id")
w_grp = Window.orderBy("level_avg_salary_bucket", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w11 = Window.orderBy("running_max_job_title_sum_salary_share", "id")
w12 = Window.partitionBy(F.month("hire_date"))
w13 = Window.orderBy("running_sum_job_title_sum_salary_share", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_employees
    .filter(F.col("hire_date") >= F.to_date(F.lit("2023-08-16")))
    .select(
        "id",
        "full_name",
        "email",
        "job_title",
        "salary",
        "hire_date",
        "is_remote",
        "level",
        F.coalesce("email", F.lit("unassigned")).alias("email_filled"),
    )
)

ranked = (
    filtered
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
)

scored = (
    ranked
    .filter(F.col("job_title").isin(["Director", "Designer", "Engineer"]))
    .select(
        "id",
        "full_name",
        "email",
        "hire_date",
        "level",
        "email_filled",
        "row_num",
        F.row_number().over(w2).alias("row_seq"),
        F.ntile(5).over(w3).alias("salary_bucket"),
        F.sum("salary").over(w4).alias("job_title_sum_salary"),
    )
)

enriched = (
    scored
    .select(
        "id",
        "hire_date",
        "email_filled",
        "row_seq",
        "salary_bucket",
        F.avg("job_title_sum_salary").over(w5).alias("job_title_sum_salary_avg_3"),
        F.row_number().over(w6).alias("seq_no"),
        F.round((F.col("job_title_sum_salary") / F.sum("job_title_sum_salary").over(w7)), 4).alias("job_title_sum_salary_share"),
        F.avg("salary_bucket").over(w8).alias("level_avg_salary_bucket"),
        F.row_number().over(w9).alias("event_seq"),
    )
)

with_windows = (
    enriched
    .select(
        "*",
        F.sum("job_title_sum_salary_share").over(w_grp).alias("running_sum_job_title_sum_salary_share"),
        F.min("job_title_sum_salary_share").over(w_grp).alias("running_min_job_title_sum_salary_share"),
        F.max("job_title_sum_salary_share").over(w_grp).alias("running_max_job_title_sum_salary_share"),
        F.count("job_title_sum_salary_share").over(w_grp).alias("running_count_job_title_sum_salary_share"),
    )
)

with_windows_employees = (
    with_windows
    .withColumn("row_seq_2", F.row_number().over(w11))
    .withColumn("seq_no_share", F.round((F.col("seq_no") / F.sum("seq_no").over(w12)), 4))
    .withColumn("running_job_title_sum_salary_share", F.sum("job_title_sum_salary_share").over(w13))
)

result = (
    with_windows_employees
    .select(
        "id",
        "running_max_job_title_sum_salary_share",
        "running_sum_job_title_sum_salary_share",
        "job_title_sum_salary_share",
        "seq_no_share",
    )
    .unpivot(
        ids=["id"],
        values=["running_max_job_title_sum_salary_share", "running_sum_job_title_sum_salary_share", "job_title_sum_salary_share", "seq_no_share"],
        variableColumnName="metric",
        valueColumnName="value",
    )
    .filter(F.col("value").isNotNull())
)
