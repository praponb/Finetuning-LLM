from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("manager_id").orderBy("hire_date", "id")
w_part = Window.partitionBy("manager_id").orderBy("salary", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.orderBy("salary", "id").rowsBetween(-2, Window.currentRow)
w_grp = Window.partitionBy("level", "manager_id").orderBy("salary", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("manager_id").orderBy("next_salary", "id")
w6 = Window.orderBy("prev_salary_avg_2", "id").rowsBetween(-2, -1)
w_ordered = Window.partitionBy("level").orderBy("salary_avg_2", "id").rowsBetween(-5, -1)
w8 = Window.partitionBy("manager_id", "level")
w9 = Window.partitionBy("level")
w10 = Window.partitionBy("manager_id_count_salary_tier").orderBy(F.col("avg_manager_id_count_salary_share").desc())

base = (
    df_employees
    .filter(F.col("job_title").isin(["Engineer", "Intern", "Manager"]))
    .select(
        "id",
        "full_name",
        "email",
        "manager_id",
        "salary",
        "hire_date",
        "is_remote",
        "level",
        F.trim("job_title").alias("clean_job_title"),
        F.coalesce("level", F.lit("NONE")).alias("resolved_level"),
    )
)

scored = (
    base
    .filter(F.col("email").like("%@GMAIL.COM"))
    .select(
        "*",
        F.lead("salary", 1).over(w).alias("next_salary"),
        F.first("level", ignorenulls=True).over(w_part).alias("first_known_level"),
        F.avg("salary").over(w3).alias("salary_avg_2"),
    )
)

metrics = (
    scored
    .select(
        "*",
        F.min("salary").over(w_grp).alias("running_min_salary"),
        F.max("salary").over(w_grp).alias("running_max_salary"),
        F.sum("salary").over(w_grp).alias("running_sum_salary"),
        F.lag("salary_avg_2", 1).over(w5).alias("prev_salary_avg_2"),
    )
)

with_windows = (
    metrics
    .select(
        "id",
        "full_name",
        "salary",
        "is_remote",
        "level",
        "clean_job_title",
        "resolved_level",
        "running_max_salary",
        "prev_salary_avg_2",
        F.avg("running_max_salary").over(w6).alias("running_max_salary_avg_2_prior"),
        F.avg("running_sum_salary").over(w_ordered).alias("running_sum_salary_avg_5_prior"),
        F.count("salary").over(w8).alias("manager_id_count_salary"),
    )
)

kept_keys = (
    with_windows
    .select("id", "level")
    .subtract(
        df_employees
        .filter(F.col("manager_id").isNotNull())
        .select("id", "level")
    )
)

kept_rows = (
    with_windows
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.level") == F.col("k.level")), "inner")
    .select("s.*")
)

base_employees = (
    kept_rows
    .filter(F.col("level").isNotNull())
    .select(
        "id",
        "full_name",
        "salary",
        "is_remote",
        "level",
        "clean_job_title",
        "resolved_level",
        "running_sum_salary_avg_5_prior",
        "manager_id_count_salary",
        F.when(F.col("level").isin(["L1", "L4"]), "Core").otherwise("Other").alias("level_group"),
    )
)

scored_employees = (
    base_employees
    .select(
        "*",
        F.count("running_sum_salary_avg_5_prior").over(w9).alias("level_count_running_sum_salary_avg_5_prior"),
        F.round((F.col("manager_id_count_salary") / F.sum("manager_id_count_salary").over(w9)), 4).alias("manager_id_count_salary_share"),
    )
)

result = (
    scored_employees
    .groupBy(
        F.when(F.col("manager_id_count_salary") >= 15, "LARGE").otherwise("SMALL").alias("manager_id_count_salary_tier"),
        F.when(F.col("level_count_running_sum_salary_avg_5_prior") >= 15, "HIGH").otherwise("LOW").alias("level_count_running_sum_salary_avg_5_prior_tier"),
        "level",
    )
    .agg(
        F.round(F.avg("manager_id_count_salary"), 2).alias("avg_manager_id_count_salary"),
        F.avg("manager_id_count_salary_share").alias("avg_manager_id_count_salary_share"),
        F.percentile_approx("salary", 0.95).alias("salary_p95"),
        F.stddev_samp("salary").alias("salary_stddev"),
        F.sum("running_sum_salary_avg_5_prior").alias("total_running_sum_salary_avg_5_prior"),
    )
    .withColumn("avg_manager_id_count_salary_share_dense_rank", F.dense_rank().over(w10))
    .orderBy("level")
)
