from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy("job_title").orderBy("salary", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("job_title")

cleaned = (
    df_employees
    .filter(F.col("hire_date") > F.to_date(F.lit("2023-06-20")))
    .select(
        "id",
        "full_name",
        "job_title",
        "salary",
        "is_remote",
        F.weekofyear("hire_date").alias("hire_date_weekofyear"),
        F.coalesce("level", F.lit("UNKNOWN")).alias("resolved_level"),
    )
)

cleaned_employees = (
    cleaned
    .filter(F.col("job_title").eqNullSafe(F.lit("Intern")))
    .select(
        "id",
        "full_name",
        "job_title",
        "salary",
        "is_remote",
        "resolved_level",
        F.when(F.col("is_remote"), "Yes").otherwise("No").alias("is_remote_label"),
    )
)

enriched = (
    cleaned_employees
    .filter(F.col("is_remote"))
    .select(
        "id",
        "full_name",
        "job_title",
        "salary",
        "is_remote",
        "is_remote_label",
        F.sum("salary").over(w_recent).alias("cumulative_salary"),
        F.sum("salary").over(w_recent).alias("cumulative_salary_2"),
        F.round((F.col("salary") / F.sum("salary").over(w2)), 4).alias("salary_share"),
        F.sum("salary").over(w_recent).alias("cumulative_salary_3"),
    )
)

kept_keys = (
    enriched
    .select("job_title")
    .intersectAll(
        df_employees
        .filter(F.col("manager_id").isNull())
        .select("job_title")
    )
)

verified = (
    enriched
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.job_title") == F.col("k.job_title")), "inner")
    .select("s.*")
)

result = (
    verified
    .select(
        "id",
        "full_name",
        "job_title",
        "salary",
        "is_remote",
        "cumulative_salary",
        "cumulative_salary_2",
        "cumulative_salary_3",
    )
    .orderBy(F.col("job_title").desc())
)
