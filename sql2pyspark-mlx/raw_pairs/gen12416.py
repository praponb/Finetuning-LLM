from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("location")
w2 = Window.partitionBy("dept_name").orderBy("budget", "id")
w3 = Window.partitionBy("dept_name").orderBy("budget", "id").rowsBetween(-6, -1)
w4 = Window.partitionBy("dept_name", "location").orderBy("budget", "id")
w5 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.hire_date").desc(), F.col("c.id").desc())
w6 = Window.partitionBy("location").orderBy(F.col("budget").desc(), "id")
w7 = Window.partitionBy("level", "location").orderBy(F.col("seq_no").desc(), "id")
w8 = Window.partitionBy("level", "location")
w9 = Window.partitionBy("location").orderBy("seq_no", "id")
w10 = Window.partitionBy("location").orderBy("seq_no_bucket")

with_windows = (
    df_departments
    .filter(F.col("location").isin(["Berlin", "Bangkok", "Singapore"]))
    .select(
        "id",
        "location",
        "budget",
        F.count("budget").over(w).alias("location_count_budget"),
        F.round((F.col("budget") / F.sum("budget").over(w)), 4).alias("budget_share"),
        F.ntile(3).over(w2).alias("budget_bucket"),
        F.avg("budget").over(w3).alias("budget_avg_6_prior"),
        F.row_number().over(w4).alias("seq_no"),
    )
)

filtered = (
    with_windows
    .filter(F.col("budget") >= 3873000)
    .withColumn("budget_minus_budget_avg_6_prior", (F.col("budget") - F.col("budget_avg_6_prior")))
    .withColumn("budget_band", F.when(F.col("budget") >= 3530000, "High").when(F.col("budget") >= 1658000, "Medium").otherwise("Low"))
    .withColumn("location_clean", F.coalesce("location", F.lit("N/A")))
)

with_latest = (
    filtered
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w5))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.level"), F.col("c.hire_date").alias("employee_hire_date"))
)

deduped = (
    with_latest
    .withColumn("latest_rank", F.row_number().over(w6))
    .filter(F.col("latest_rank") == 1)
    .select(
        "id",
        "location",
        "location_count_budget",
        "budget_bucket",
        "budget_avg_6_prior",
        "seq_no",
        "budget_minus_budget_avg_6_prior",
        "budget_band",
        "location_clean",
        "level",
    )
)

kept = (
    deduped
    .alias("s")
    .join(df_employees.alias("x"), (F.col("x.dept_id") == F.col("s.id")) & (F.col("x.full_name").like("%Emm%")), "left_semi")
)

metrics = (
    kept
    .select(
        "id",
        "location",
        "location_count_budget",
        "budget_bucket",
        "budget_avg_6_prior",
        "seq_no",
        "budget_band",
        F.row_number().over(w7).alias("event_seq"),
        F.round((F.col("budget_bucket") / F.sum("budget_bucket").over(w8)), 4).alias("budget_bucket_share"),
        F.ntile(3).over(w9).alias("seq_no_bucket"),
    )
)

scored = (
    metrics
    .select(
        "id",
        "location_count_budget",
        "budget_bucket",
        "seq_no",
        "budget_band",
        "budget_bucket_share",
        F.dense_rank().over(w10).alias("seq_no_bucket_dense_rank"),
        F.row_number().over(w9).alias("row_seq"),
    )
)

result = (
    scored
    .groupBy("seq_no")
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.avg("budget_bucket").alias("avg_budget_bucket"),
        F.avg("seq_no").alias("avg_seq_no"),
        F.percentile_approx("seq_no", [0.25, 0.5, 0.75]).alias("seq_no_quartiles"),
        F.round(F.avg("seq_no"), 2).alias("avg_seq_no_2"),
    )
    .orderBy(F.col("seq_no").desc())
)
