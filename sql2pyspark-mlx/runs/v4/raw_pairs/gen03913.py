from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_id").orderBy("salary", "id").rowsBetween(-5, -1)
w2 = Window.partitionBy("job_title")
w3 = Window.partitionBy("dept_id", "level")
w4 = Window.partitionBy("dept_id").orderBy(F.col("salary").desc())
w5 = Window.partitionBy("job_title").orderBy(F.col("dept_id_stddev_samp_salary").desc_nulls_first())
w6 = Window.orderBy("rank_in_group", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy("manager_id").orderBy("salary", "id")
w_hist = Window.partitionBy("dept_id").orderBy("salary_avg_5_prior", "id")
w9 = Window.partitionBy("level", "job_title").orderBy("salary_avg_5_prior", "id")

metrics = (
    df_employees
    .select(
        "*",
        F.avg("salary").over(w).alias("salary_avg_5_prior"),
        F.round((F.col("salary") / F.sum("salary").over(w2)), 4).alias("salary_share"),
        F.stddev_samp("salary").over(w3).alias("dept_id_stddev_samp_salary"),
    )
)

deduped = (
    metrics
    .withColumn("rank_in_group", F.rank().over(w4))
    .filter(F.col("rank_in_group") == 1)
)

joined = (
    deduped
    .alias("s")
    .join(df_employees.alias("e"), F.col("s.manager_id") == F.col("e.id"), "inner")
    .select("s.*", F.col("e.job_title").alias("employee_job_title"))
)

enriched = (
    joined
    .select(
        "*",
        F.dense_rank().over(w5).alias("dept_id_stddev_samp_salary_dense_rank"),
        F.last("salary_share", ignorenulls=True).over(w6).alias("last_known_salary_share"),
        F.lead("dept_id_stddev_samp_salary", 1).over(w7).alias("next_dept_id_stddev_samp_salary"),
        F.row_number().over(w_hist).alias("row_seq"),
    )
)

deduped_employees = (
    enriched
    .withColumn("dedup_rank", F.row_number().over(w9))
    .filter(F.col("dedup_rank") == 1)
)

result = (
    deduped_employees
    .groupBy("dept_id")
    .agg(
        F.count(F.when(F.col("job_title") == "Analyst", 1)).alias("analyst_rows"),
        F.round(F.avg("row_seq"), 2).alias("avg_row_seq"),
        F.count_if(F.col("employee_job_title") == "Analyst").alias("analyst_events"),
        F.count("*").alias("row_count"),
        F.sum(F.when(F.col("job_title") == "Manager", F.col("dept_id_stddev_samp_salary"))).alias("manager_total_dept_id_stddev_samp_salary"),
        F.percentile_approx("row_seq", [0.25, 0.5, 0.75]).alias("row_seq_quartiles"),
    )
    .orderBy("analyst_events")
)
