from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("manager_id").orderBy("hire_date", "id")
w2 = Window.partitionBy("employee_job_title").orderBy(F.col("hire_date").desc(), "id")
w3 = Window.partitionBy("level").orderBy(F.col("salary").desc())
w4 = Window.orderBy("salary", "id").rowsBetween(-3, -1)

filtered = (
    df_employees
    .filter(F.col("level").isNotNull())
    .select(
        "*",
        F.concat_ws("-", "full_name", "job_title").alias("full_name_job_title_key"),
    )
)

ranked = (
    filtered
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
    .drop("latest_rank")
)

enriched = (
    ranked
    .alias("a")
    .join(df_employees.alias("d"), F.col("a.manager_id") == F.col("d.id"), "left")
    .select("a.*", F.col("d.job_title").alias("employee_job_title"))
)

ranked_employees = (
    enriched
    .withColumn("row_num", F.row_number().over(w2))
    .filter(F.col("row_num") == 1)
)

scored = (
    ranked_employees
    .withColumn("salary_cume_dist", F.cume_dist().over(w3))
    .withColumn("salary_avg_3_prior", F.avg("salary").over(w4))
)

summary = (
    scored
    .groupBy(F.date_trunc("MONTH", "hire_date").alias("activity_month"))
    .agg(
        F.sum(F.when(F.col("job_title") == "Designer", F.col("salary")).otherwise(0)).alias("designer_salary"),
        F.count_if(F.col("level") == "L4").alias("l4_events"),
        F.percentile_approx("salary_avg_3_prior", [0.25, 0.5, 0.75]).alias("salary_avg_3_prior_quartiles"),
        F.countDistinct("dept_id").alias("distinct_dept_ids"),
    )
    .filter(F.col("distinct_dept_ids") < 1)
)

result = (
    summary
    .select(
        "activity_month",
        "designer_salary",
        "l4_events",
        "salary_avg_3_prior_quartiles",
    )
    .orderBy(F.col("l4_events").desc(), "designer_salary")
)
