from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("job_title", "level").orderBy("salary", "id").rowsBetween(-6, -1)
w2 = Window.partitionBy(F.month("hire_date"))
w3 = Window.orderBy(F.col("hire_date").desc(), "id")
w4 = Window.orderBy(F.col("salary").desc(), "id")
w5 = Window.partitionBy("dept_id").orderBy("salary")

with_windows = (
    df_employees
    .filter(F.length("email") > 7)
    .select(
        "*",
        F.avg("salary").over(w).alias("salary_avg_6_prior"),
        F.stddev_samp("salary").over(w2).alias("hire_date_month_stddev_samp_salary"),
        F.row_number().over(w3).alias("seq_no"),
        F.ntile(3).over(w4).alias("salary_bucket"),
        F.percent_rank().over(w5).alias("salary_percent_rank"),
    )
)

result = (
    with_windows
    .groupBy(
        F.when(F.col("salary_percent_rank") >= 0.82, "PREMIUM").otherwise("STANDARD").alias("salary_percent_rank_tier"),
    )
    .agg(
        F.count_if(F.col("job_title") == "Manager").alias("manager_events"),
        F.percentile_approx("hire_date_month_stddev_samp_salary", 0.95).alias("hire_date_month_stddev_samp_salary_p95"),
    )
    .orderBy("manager_events")
)
