from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level").orderBy(F.col("salary").desc(), "id")
w2 = Window.partitionBy("manager_id").orderBy("budget_times_dedup_rank", "id").rowsBetween(-2, Window.currentRow)

valid = (
    df_employees
    .filter((F.col("hire_date") >= F.lit("2023-05-05").cast("date")) & F.col("job_title").isNotNull())
    .withColumn("job_title_group", F.when(F.col("job_title").isin(["Designer", "Manager"]), "Group A").otherwise("Other"))
)

joined = (
    valid
    .alias("s")
    .join(df_departments.alias("b"), F.col("s.dept_id") == F.col("b.id"), "left")
    .filter(F.col("b.dept_name").isin(["Marketing", "HR", "Engineering"]))
    .select("s.*", F.col("b.budget"))
)

deduped = (
    joined
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
)

filtered = (
    deduped
    .filter(F.col("level").isNotNull())
    .select(
        "id",
        "full_name",
        "email",
        "manager_id",
        "level",
        "job_title_group",
        "budget",
        "dedup_rank",
        F.coalesce("full_name", F.lit("N/A")).alias("full_name_filled"),
        F.coalesce("job_title", F.lit("DIRECT")).alias("job_title_filled"),
    )
)

cleaned = (
    filtered
    .filter(F.col("manager_id").isNotNull())
    .withColumn("dedup_rank_plus_budget", (F.col("dedup_rank") + F.col("budget")))
    .withColumn("budget_times_dedup_rank", (F.col("budget") * F.col("dedup_rank")))
    .withColumn("level_filled", F.coalesce("level", F.lit("DIRECT")))
)

scored = (
    cleaned
    .select(
        "*",
        F.max("dedup_rank_plus_budget").over(w2).alias("last3_max_dedup_rank_plus_budget"),
        F.sum("dedup_rank_plus_budget").over(w2).alias("last3_sum_dedup_rank_plus_budget"),
        F.avg("dedup_rank_plus_budget").over(w2).alias("last3_avg_dedup_rank_plus_budget"),
    )
)

result = (
    scored
    .select("id", "level", "budget", "last3_sum_dedup_rank_plus_budget")
    .unpivot(
        ids=["id", "level"],
        values=["budget", "last3_sum_dedup_rank_plus_budget"],
        variableColumnName="metric_name",
        valueColumnName="val",
    )
    .filter(F.col("val").isNotNull())
)
