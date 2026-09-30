from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level").orderBy("salary", "id")
w_grp = Window.partitionBy("manager_id").orderBy("salary", "id")
w3 = Window.partitionBy("level")
w4 = Window.partitionBy("dept_id").orderBy("latest_rank", "id")
w5 = Window.orderBy("latest_rank", "id")
w6 = Window.partitionBy("level", "manager_id").orderBy("salary_share", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy("level").orderBy(F.col("first_known_salary_share").desc_nulls_first(), "id")
w_part = Window.partitionBy("level", "manager_id").orderBy("prev_salary_2", "id")

deduped = (
    df_employees
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
)

metrics = (
    deduped
    .filter(F.col("dept_id") == 35)
    .select(
        "*",
        F.lead("salary").over(w_grp).alias("next_salary"),
        F.lag("salary").over(w_grp).alias("prev_salary"),
        F.row_number().over(w_grp).alias("row_seq"),
        F.round((F.col("salary") / F.sum("salary").over(w3)), 4).alias("salary_share"),
    )
)

scored = (
    metrics
    .select(
        "id",
        "email",
        "manager_id",
        "job_title",
        "hire_date",
        "is_remote",
        "level",
        "latest_rank",
        "next_salary",
        "prev_salary",
        F.lag("salary_share", 1).over(w4).alias("prev_salary_share"),
        F.lag("salary", 1).over(w5).alias("prev_salary_2"),
        F.first("salary_share", ignorenulls=True).over(w6).alias("first_known_salary_share"),
    )
)

latest = (
    scored
    .withColumn("rank_in_group", F.row_number().over(w7))
    .filter(F.col("rank_in_group") == 1)
)

with_windows = (
    latest
    .select(
        "id",
        "manager_id",
        "job_title",
        "is_remote",
        "level",
        "latest_rank",
        "prev_salary",
        "prev_salary_2",
        "rank_in_group",
        F.lead("prev_salary").over(w_part).alias("next_prev_salary"),
        F.lag("prev_salary").over(w_part).alias("prev_prev_salary"),
        F.lead("prev_salary_2").over(w_part).alias("next_prev_salary_2"),
        F.row_number().over(w_part).alias("event_seq"),
    )
)

summary = (
    with_windows
    .withColumn("prev_salary_tier", F.when(F.col("prev_salary") >= 138000, "LARGE").otherwise("SMALL"))
    .rollup("prev_salary_tier", "job_title")
    .agg(
        F.round(F.avg("next_prev_salary_2"), 2).alias("avg_next_prev_salary_2"),
        F.size(F.collect_set("level")).alias("num_levels"),
        F.count(F.when(F.col("level") == "L3", 1)).alias("l3_count"),
        F.percentile_approx("next_prev_salary", [0.25, 0.5, 0.75]).alias("next_prev_salary_quartiles"),
        F.sum(F.when(F.col("job_title") == "Intern", F.col("event_seq")).otherwise(0)).alias("intern_event_seq"),
        F.grouping("prev_salary_tier").alias("gid"),
    )
)

result = (
    summary
    .select(
        "prev_salary_tier",
        "job_title",
        "num_levels",
        "l3_count",
        "intern_event_seq",
        "gid",
    )
)
