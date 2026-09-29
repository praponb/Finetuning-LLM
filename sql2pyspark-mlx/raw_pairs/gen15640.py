from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_id", "manager_id")
w2 = Window.orderBy(F.col("salary").desc())
w3 = Window.partitionBy("level")
w4 = Window.partitionBy("dept_id")
w5 = Window.partitionBy("dept_id", "manager_id").orderBy("salary")

valid = (
    df_employees
    .filter((F.col("hire_date") > F.lit("2023-07-16").cast("date")) & (F.col("hire_date") <= "2023-06-18"))
    .select(
        "id",
        "full_name",
        "email",
        "dept_id",
        "manager_id",
        "salary",
        "hire_date",
        "is_remote",
        "level",
        F.when(F.col("job_title").isin(["Designer", "Director"]), "Tier 1").otherwise("Other").alias("job_title_group"),
    )
)

matched_keys = (
    valid
    .select("id")
    .exceptAll(
        df_employees
        .filter(F.col("manager_id").isNotNull())
        .select("id")
    )
)

kept_rows = (
    valid
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

scored = (
    kept_rows
    .select(
        "full_name",
        "email",
        "hire_date",
        "is_remote",
        "level",
        F.round((F.col("salary") / F.sum("salary").over(w)), 4).alias("salary_share"),
        F.percent_rank().over(w2).alias("salary_percent_rank"),
        F.sum("salary").over(w3).alias("level_sum_salary"),
        F.round((F.col("salary") / F.sum("salary").over(w4)), 4).alias("salary_share_2"),
        F.percent_rank().over(w5).alias("salary_percent_rank_2"),
    )
)

cleaned = (
    scored
    .filter(F.col("hire_date") > F.lit("2023-08-25").cast("date"))
    .select(
        "email",
        "hire_date",
        "is_remote",
        "level",
        "salary_share",
        "salary_percent_rank",
        "level_sum_salary",
        "salary_share_2",
        "salary_percent_rank_2",
        F.round((F.col("level_sum_salary") * 1.15), 2).alias("adjusted_level_sum_salary"),
        F.coalesce("level", F.lit("NONE")).alias("level_filled"),
    )
)

cleaned_employees = (
    cleaned
    .filter((F.col("hire_date") >= F.to_date(F.lit("2023-08-07"))) & F.col("level").isNotNull())
    .select(
        "is_remote",
        "salary_percent_rank",
        "level_sum_salary",
        "salary_share_2",
        "salary_percent_rank_2",
        "level_filled",
        F.round("adjusted_level_sum_salary", 0).alias("adjusted_level_sum_salary_rounded"),
        F.coalesce("level", F.lit("NONE")).alias("resolved_level"),
    )
)

result = (
    cleaned_employees
    .select(
        "is_remote",
        "salary_percent_rank",
        "level_sum_salary",
        "salary_share_2",
        "salary_percent_rank_2",
        "level_filled",
        "resolved_level",
    )
    .orderBy("salary_share_2")
)
