from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("manager_id").orderBy("hire_date", "id")
w2 = Window.partitionBy("job_title", "dept_id")
w_running = Window.partitionBy("level").orderBy("next_salary", "id")
w4 = Window.orderBy(F.col("hire_date").desc(), "id")
w5 = Window.partitionBy("level").orderBy("row_seq", "id")
w6 = Window.partitionBy("manager_id").orderBy("hire_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

enriched = (
    df_employees
    .select(
        "*",
        F.lead("salary", 1).over(w).alias("next_salary"),
        F.round((F.col("salary") / F.sum("salary").over(w2)), 4).alias("salary_share"),
    )
)

scored = (
    enriched
    .withColumn("next_salary_share", F.lead("salary_share", 1).over(w_running))
    .withColumn("row_seq", F.row_number().over(w4))
)

scored_employees = (
    scored
    .select(
        "*",
        F.lag("next_salary_share", 1).over(w5).alias("prev_next_salary_share"),
        F.last("job_title", ignorenulls=True).over(w6).alias("last_known_job_title"),
    )
)

result = (
    scored_employees
    .groupBy(
        F.when(F.col("prev_next_salary_share") >= 0.61, "PREMIUM").otherwise("STANDARD").alias("prev_next_salary_share_tier"),
        "job_title",
    )
    .agg(
        F.max(F.when(F.col("level") == "L1", F.col("salary_share"))).alias("l1_max_salary_share"),
        F.sum("salary_share").alias("total_salary_share"),
        F.percentile_approx("salary_share", [0.25, 0.5, 0.75]).alias("salary_share_quartiles"),
        F.sum(F.when(F.col("job_title") == "Designer", F.col("salary")).otherwise(0)).alias("designer_salary"),
        F.percentile_approx("row_seq", [0.25, 0.5, 0.75]).alias("row_seq_quartiles"),
    )
    .filter(F.col("l1_max_salary_share") >= 0)
    .orderBy(F.col("job_title").asc_nulls_last())
)
