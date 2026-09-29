from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("job_title").orderBy("salary_rounded", "id")
w2 = Window.partitionBy("level").orderBy("salary_rounded", "id").rowsBetween(-6, -1)
w3 = Window.partitionBy("job_title").orderBy(F.col("salary_rounded_int").desc(), "id")
w4 = Window.partitionBy("job_title")
w_part = Window.partitionBy("dept_id").orderBy("salary_rounded", "id")
w6 = Window.partitionBy("dept_id").orderBy("salary_rounded_avg_6_prior", "id")

base = (
    df_employees
    .filter(F.col("level").isNotNull())
    .select(
        "id",
        "email",
        "dept_id",
        "manager_id",
        "job_title",
        "salary",
        "is_remote",
        "level",
        F.trim("job_title").alias("clean_job_title"),
    )
)

cleaned = (
    base
    .filter(~F.col("level").eqNullSafe(F.lit("L2")))
    .select(
        "id",
        "dept_id",
        "manager_id",
        "job_title",
        "is_remote",
        "level",
        "clean_job_title",
        F.round("salary", 0).alias("salary_rounded"),
    )
)

cleaned_employees = (
    cleaned
    .filter((~F.col("level").eqNullSafe(F.lit("L2"))) & F.col("manager_id").isin([18, 25, 28]))
    .select(
        "id",
        "dept_id",
        "manager_id",
        "job_title",
        "is_remote",
        "level",
        "clean_job_title",
        "salary_rounded",
        F.col("salary_rounded").cast("int").alias("salary_rounded_int"),
        F.when(F.col("is_remote"), "Yes").otherwise("No").alias("is_remote_label"),
        F.coalesce("level", F.lit("NONE")).alias("resolved_level"),
    )
)

scored = (
    cleaned_employees
    .select(
        "id",
        "dept_id",
        "manager_id",
        "is_remote",
        "clean_job_title",
        "salary_rounded",
        "resolved_level",
        F.row_number().over(w).alias("seq_no"),
        F.avg("salary_rounded").over(w2).alias("salary_rounded_avg_6_prior"),
        F.row_number().over(w3).alias("event_seq"),
        F.max("salary_rounded_int").over(w4).alias("job_title_max_salary_rounded_int"),
    )
)

enriched = (
    scored
    .alias("a")
    .join(df_employees.alias("b"), F.col("a.manager_id") == F.col("b.id"), "left")
    .select("a.*", F.col("b.email"), F.col("b.level"))
)

enriched_employees = (
    enriched
    .select(
        "id",
        "dept_id",
        "manager_id",
        "resolved_level",
        "email",
        F.lead("job_title_max_salary_rounded_int").over(w_part).alias("next_job_title_max_salary_rounded_int"),
        F.lag("salary_rounded").over(w_part).alias("prev_salary_rounded"),
        F.row_number().over(w_part).alias("row_seq"),
        F.ntile(5).over(w6).alias("salary_rounded_avg_6_prior_bucket"),
    )
)

valid = (
    enriched_employees
    .filter(F.col("email").isNotNull())
    .select(
        "id",
        "dept_id",
        "manager_id",
        "email",
        "next_job_title_max_salary_rounded_int",
        "prev_salary_rounded",
        "row_seq",
        "salary_rounded_avg_6_prior_bucket",
        F.round((F.col("prev_salary_rounded") / F.col("salary_rounded_avg_6_prior_bucket")), 2).alias("prev_salary_rounded_per_salary_rounded_avg_6_prior_bucket"),
        F.regexp_extract("email", "@(.+)$", 1).alias("email_domain"),
    )
)

result = (
    valid
    .groupBy(
        "manager_id",
        F.when(F.col("next_job_title_max_salary_rounded_int") >= 89, "PREMIUM").otherwise("STANDARD").alias("next_job_title_max_salary_rounded_int_tier"),
        F.when(F.col("prev_salary_rounded_per_salary_rounded_avg_6_prior_bucket") >= 87, "HIGH").otherwise("LOW").alias("prev_salary_rounded_per_salary_rounded_avg_6_prior_bucket_tier"),
    )
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.percentile_approx("row_seq", [0.25, 0.5, 0.75]).alias("row_seq_quartiles"),
        F.max("prev_salary_rounded_per_salary_rounded_avg_6_prior_bucket").alias("max_prev_salary_rounded_per_salary_rounded_avg_6_prior_bucket"),
        F.round(F.avg("prev_salary_rounded"), 2).alias("avg_prev_salary_rounded"),
    )
    .orderBy("manager_id")
)
