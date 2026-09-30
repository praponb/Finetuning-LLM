from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("salary").desc())
w_running = Window.orderBy("hire_date", "id")
w_part = Window.partitionBy(F.date_trunc("WEEK", "hire_date")).orderBy("hire_date", "id").rowsBetween(-5, -1)
w4 = Window.orderBy("row_seq_plus_salary_cume_dist", "id")
w5 = Window.orderBy("row_seq", "id")

cleaned = (
    df_employees
    .filter(F.col("manager_id").isNotNull() & (~F.col("level").eqNullSafe(F.lit("L4"))))
    .select(
        "id",
        "full_name",
        "email",
        "salary",
        "hire_date",
        "is_remote",
        F.coalesce("level", F.lit("unassigned")).alias("level_filled"),
    )
)

scored = (
    cleaned
    .select(
        "id",
        "email",
        "is_remote",
        "level_filled",
        F.cume_dist().over(w).alias("salary_cume_dist"),
        F.row_number().over(w_running).alias("row_seq"),
        F.avg("salary").over(w_part).alias("salary_avg_5_prior"),
    )
)

filtered = (
    scored
    .filter(F.col("salary_avg_5_prior") < 131275)
    .select(
        "*",
        (F.col("row_seq") + F.col("salary_cume_dist")).alias("row_seq_plus_salary_cume_dist"),
    )
)

scored_employees = (
    filtered
    .select(
        "id",
        "row_seq_plus_salary_cume_dist",
        F.lead("row_seq_plus_salary_cume_dist").over(w4).alias("next_row_seq_plus_salary_cume_dist"),
        F.lead("salary_avg_5_prior").over(w4).alias("next_salary_avg_5_prior"),
        F.lag("salary_avg_5_prior").over(w4).alias("prev_salary_avg_5_prior"),
        F.row_number().over(w4).alias("row_seq_2"),
        F.lead("salary_avg_5_prior", 1).over(w5).alias("next_salary_avg_5_prior_2"),
    )
)

result = (
    scored_employees
    .select(
        "id",
        "row_seq_plus_salary_cume_dist",
        "next_row_seq_plus_salary_cume_dist",
        "next_salary_avg_5_prior",
        "next_salary_avg_5_prior_2",
    )
    .orderBy(
        F.col("next_row_seq_plus_salary_cume_dist").desc(),
        "row_seq_plus_salary_cume_dist",
    )
)
