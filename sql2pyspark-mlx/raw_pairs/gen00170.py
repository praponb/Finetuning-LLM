from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_id").orderBy("hire_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_part = Window.partitionBy("job_title").orderBy("salary", "id").rowsBetween(-2, Window.currentRow)
w3 = Window.partitionBy("dept_id")
w4 = Window.partitionBy("dept_id", "level").orderBy("hire_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("dept_id").orderBy("salary")
w6 = Window.partitionBy("job_title").orderBy(F.col("salary_avg_2").desc(), "id")
w_hist = Window.partitionBy("job_title").orderBy("salary_dense_rank", "id")
w8 = Window.partitionBy("job_title").orderBy(F.col("next_salary_avg_2_bucket").desc(), "id")
w9 = Window.orderBy("prev_salary_avg_2_bucket", "id")
w10 = Window.orderBy("prev_salary_avg_2_bucket", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

enriched = (
    df_employees
    .select(
        "id",
        "full_name",
        "email",
        "dept_id",
        "job_title",
        "salary",
        "hire_date",
        F.first("job_title", ignorenulls=True).over(w).alias("first_known_job_title"),
        F.avg("salary").over(w_part).alias("salary_avg_2"),
        F.max("salary").over(w3).alias("dept_id_max_salary"),
        F.first("job_title", ignorenulls=True).over(w4).alias("first_known_job_title_2"),
    )
)

enriched_employees = (
    enriched
    .select(
        "id",
        "job_title",
        "hire_date",
        F.dense_rank().over(w5).alias("salary_dense_rank"),
        F.ntile(4).over(w6).alias("salary_avg_2_bucket"),
    )
)

with_windows = (
    enriched_employees
    .select(
        "id",
        "job_title",
        "salary_avg_2_bucket",
        F.lead("salary_avg_2_bucket").over(w_hist).alias("next_salary_avg_2_bucket"),
        F.lag("salary_avg_2_bucket").over(w_hist).alias("prev_salary_avg_2_bucket"),
        F.lag("salary_dense_rank").over(w_hist).alias("prev_salary_dense_rank"),
        F.lead("salary_dense_rank").over(w_hist).alias("next_salary_dense_rank"),
        F.row_number().over(w_hist).alias("row_seq"),
    )
)

deduped = (
    with_windows
    .withColumn("rank_in_group", F.row_number().over(w8))
    .filter(F.col("rank_in_group") == 1)
    .select(
        "id",
        "salary_avg_2_bucket",
        "next_salary_avg_2_bucket",
        "prev_salary_avg_2_bucket",
        "next_salary_dense_rank",
        "row_seq",
    )
)

result = (
    deduped
    .select(
        "id",
        "next_salary_avg_2_bucket",
        "row_seq",
        F.row_number().over(w9).alias("event_seq"),
        F.sum("salary_avg_2_bucket").over(w10).alias("cumulative_salary_avg_2_bucket"),
    )
)
