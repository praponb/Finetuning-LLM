from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("manager_id").orderBy(F.col("hire_date").desc_nulls_last(), "id")
w2 = Window.partitionBy(F.year("hire_date"), "job_title").orderBy("salary", "id").rowsBetween(-3, -1)
w_ordered = Window.partitionBy("manager_id")
w_part = Window.partitionBy("manager_id").orderBy("manager_id_min_salary", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_running = Window.partitionBy("level")

ranked = (
    df_employees
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
    .drop("dedup_rank")
)

enriched = (
    ranked
    .alias("a")
    .join(df_departments.alias("p"), F.col("a.dept_id") == F.col("p.id"), "left")
    .select("a.*", F.col("p.dept_name"))
)

metrics = (
    enriched
    .filter(F.col("dept_id").isin([12, 32]))
    .select(
        "id",
        "manager_id",
        "is_remote",
        F.avg("salary").over(w2).alias("salary_avg_3_prior"),
        F.min("salary").over(w_ordered).alias("manager_id_min_salary"),
    )
)

scored = (
    metrics
    .select(
        "*",
        F.max("manager_id_min_salary").over(w_part).alias("running_max_manager_id_min_salary"),
        F.count("manager_id_min_salary").over(w_part).alias("running_count_manager_id_min_salary"),
        F.sum("manager_id_min_salary").over(w_part).alias("running_sum_manager_id_min_salary"),
    )
)

with_dims = (
    scored
    .alias("a")
    .join(df_employees.alias("p"), F.col("a.manager_id").eqNullSafe(F.col("p.id")), "left")
    .select("a.*", F.col("p.job_title"), F.col("p.level"), F.col("p.full_name"))
)

kept_keys = (
    with_dims
    .select("manager_id", "job_title")
    .intersect(
        df_employees
        .filter(F.col("dept_id").isin([10, 29, 34]))
        .select("manager_id", "job_title")
    )
)

reconciled = (
    with_dims
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.manager_id") == F.col("k.manager_id")) & (F.col("s.job_title") == F.col("k.job_title")), "inner")
    .select("s.*")
)

metrics_employees = (
    reconciled
    .withColumn("level_count_manager_id_min_salary", F.count("manager_id_min_salary").over(w_running))
)

result = (
    metrics_employees
    .alias("src")
    .join(df_employees.alias("dim"), F.col("src.manager_id") == F.col("dim.id"), "inner")
    .groupBy(
        F.col("dim.level").alias("employee_level"),
        F.col("dim.job_title").alias("employee_job_title"),
    )
    .agg(
        F.count("*").alias("rows_total"),
        F.round(F.avg(F.col("src.level_count_manager_id_min_salary")), 2).alias("avg_level_count_manager_id_min_salary"),
    )
    .orderBy(F.col("rows_total").asc_nulls_first())
)
