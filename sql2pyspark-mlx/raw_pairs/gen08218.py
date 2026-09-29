from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.partitionBy("manager_id", "level").orderBy("salary", "id").rowsBetween(-6, Window.currentRow)
w2 = Window.partitionBy("job_title", "level")
w3 = Window.partitionBy("level").orderBy("salary", "id")
w_hist = Window.partitionBy("level").orderBy("next_last7_avg_salary", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy(F.year("hire_date")).orderBy(F.col("last7_sum_salary").desc_nulls_last())
w6 = Window.partitionBy("level").orderBy("last7_sum_salary", "id")

cleaned = (
    df_employees
    .filter(F.col("manager_id").isNotNull() & (F.length("full_name") >= 9))
    .select(
        "id",
        "full_name",
        "email",
        "manager_id",
        "job_title",
        "salary",
        "hire_date",
        "is_remote",
        "level",
        F.trim("email").alias("clean_email"),
    )
)

with_windows = (
    cleaned
    .select(
        "id",
        "email",
        "salary",
        "hire_date",
        "is_remote",
        "level",
        "clean_email",
        F.avg("salary").over(w_running).alias("last7_avg_salary"),
        F.min("salary").over(w_running).alias("last7_min_salary"),
        F.sum("salary").over(w_running).alias("last7_sum_salary"),
        F.max("salary").over(w_running).alias("last7_max_salary"),
        F.round((F.col("salary") / F.sum("salary").over(w2)), 4).alias("salary_share"),
    )
)

enriched = (
    with_windows
    .select(
        "*",
        F.lead("last7_avg_salary").over(w3).alias("next_last7_avg_salary"),
        F.lag("last7_avg_salary").over(w3).alias("prev_last7_avg_salary"),
        F.lead("last7_max_salary").over(w3).alias("next_last7_max_salary"),
        F.row_number().over(w3).alias("event_seq"),
    )
)

with_windows_employees = (
    enriched
    .filter(F.col("next_last7_avg_salary") < 78000)
    .select(
        "id",
        "hire_date",
        "is_remote",
        "clean_email",
        "last7_avg_salary",
        "last7_sum_salary",
        "next_last7_avg_salary",
        "prev_last7_avg_salary",
        "event_seq",
        F.last("last7_max_salary", ignorenulls=True).over(w_hist).alias("last_known_last7_max_salary"),
        F.dense_rank().over(w5).alias("last7_sum_salary_dense_rank"),
        F.row_number().over(w6).alias("row_seq"),
    )
)

result = (
    with_windows_employees
    .select(
        "id",
        F.col("next_last7_avg_salary").alias("NEXT_LAST7_AVG_SALARY_VAL"),
        "last7_sum_salary",
    )
    .unpivot(
        ids=["id"],
        values=["NEXT_LAST7_AVG_SALARY_VAL", "last7_sum_salary"],
        variableColumnName="metric",
        valueColumnName="metric_value",
    )
    .filter(F.col("metric_value").isNotNull())
)
