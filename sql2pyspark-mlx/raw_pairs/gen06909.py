from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("location", "dept_name").orderBy(F.col("budget").desc(), "id")
w2 = Window.partitionBy("location").orderBy(F.col("rn_plus_budget").desc(), "id")
w3 = Window.partitionBy("location", "dept_name").orderBy("rn", "id")
w4 = Window.partitionBy("location").orderBy("rn_plus_budget", "id").rowsBetween(-2, -1)
w_running = Window.partitionBy("location")

latest = (
    df_departments
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
)

filtered = (
    latest
    .filter(F.col("dept_name").isNotNull() & (F.col("rn") >= 1))
    .withColumn("rn_plus_budget", (F.col("rn") + F.col("budget")))
    .withColumn("location_group", F.when(F.col("location").isin(["Singapore", "New York"]), "Priority").otherwise("Other"))
)

ranked = (
    filtered
    .withColumn("dedup_rank", F.row_number().over(w2))
    .filter(F.col("dedup_rank") == 1)
)

eligible = (
    ranked
    .alias("s")
    .join(df_employees.alias("x"), (F.col("x.dept_id") == F.col("s.id")) & (F.col("x.level").isin(["L4", "L2"])), "left_semi")
)

enriched = (
    eligible
    .select(
        "id",
        "location",
        "rn_plus_budget",
        "location_group",
        "dedup_rank",
        F.lag("rn_plus_budget").over(w3).alias("prev_rn_plus_budget"),
        F.lead("budget").over(w3).alias("next_budget"),
        F.lag("budget").over(w3).alias("prev_budget"),
        F.row_number().over(w3).alias("event_seq"),
    )
)

valid = (
    enriched
    .filter(F.col("rn_plus_budget") >= 32)
    .select(
        "*",
        F.col("rn_plus_budget").cast("int").alias("rn_plus_budget_int"),
        F.coalesce("location", F.lit("N/A")).alias("location_filled"),
    )
)

result = (
    valid
    .filter(F.col("prev_rn_plus_budget").between(36, 75))
    .select(
        "id",
        "location",
        "location_group",
        "prev_rn_plus_budget",
        "next_budget",
        "prev_budget",
        "event_seq",
        "location_filled",
        F.avg("event_seq").over(w4).alias("event_seq_avg_2_prior"),
        F.min("event_seq").over(w_running).alias("location_min_event_seq"),
    )
)
