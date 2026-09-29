from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.salary").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("job_title").orderBy(F.col("budget").desc_nulls_last(), "id")
w3 = Window.partitionBy("job_title", "level").orderBy("dedup_rank", "id").rowsBetween(-5, Window.currentRow)
w4 = Window.partitionBy("job_title").orderBy(F.col("budget").desc_nulls_last())
w5 = Window.partitionBy("dept_name").orderBy(F.col("budget").asc_nulls_first())

with_last = (
    df_departments
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.job_title"), F.col("c.level"))
)

cleaned = (
    with_last
    .filter(F.col("dept_name").isNotNull() & F.col("location").isin(["New York", "Bangkok", "Tokyo"]))
    .select(
        "*",
        F.when(F.col("level").isin(["L2", "L3"]), "Core").otherwise("Other").alias("level_group"),
    )
)

deduped = (
    cleaned
    .withColumn("dedup_rank", F.row_number().over(w2))
    .filter(F.col("dedup_rank") == 1)
)

base = (
    deduped
    .filter(F.col("job_title").isNotNull())
    .select(
        "id",
        "dept_name",
        "budget",
        "job_title",
        "level",
        "level_group",
        "dedup_rank",
        F.round("budget", 2).alias("budget_rounded"),
    )
)

scored = (
    base
    .filter(F.col("job_title") == "Manager")
    .select(
        "*",
        F.avg("budget").over(w3).alias("budget_avg_5"),
        F.cume_dist().over(w4).alias("budget_cume_dist"),
        F.cume_dist().over(w5).alias("budget_cume_dist_2"),
    )
)

result = (
    scored
    .cube("dept_name", "level")
    .agg(
        F.sum(F.when(F.col("job_title") == "Intern", F.col("budget"))).alias("intern_total_budget"),
        F.round(F.sum("budget_cume_dist_2") / F.nullif(F.count(F.when(F.col("job_title") == "Analyst", 1)), F.lit(0)), 2).alias("budget_cume_dist_2_per_analyst"),
        F.grouping("dept_name").alias("gid"),
    )
    .filter(F.col("intern_total_budget") < 1000000)
)
