from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.partitionBy("dept_name", "location").orderBy("budget", "id")
w2 = Window.orderBy("prev_budget", "id")
w3 = Window.orderBy("next_budget", "id")

scored = (
    df_departments
    .filter(F.col("budget").between(3267000, 3316000))
    .select(
        "id",
        "budget",
        F.lag("budget").over(w_running).alias("prev_budget"),
        F.lead("budget").over(w_running).alias("next_budget"),
        F.row_number().over(w_running).alias("seq_no"),
    )
)

scored_departments = (
    scored
    .select(
        "id",
        "budget",
        "prev_budget",
        F.lead("prev_budget", 1).over(w2).alias("next_prev_budget"),
        F.lag("next_budget", 2).over(w3).alias("prev_next_budget"),
        F.row_number().over(w2).alias("event_seq"),
    )
)

filtered = (
    scored_departments
    .filter(F.col("next_prev_budget") >= 2560000)
    .select(
        "id",
        "budget",
        "prev_budget",
        "event_seq",
        F.col("prev_next_budget").cast("int").alias("prev_next_budget_int"),
    )
)

base = (
    filtered
    .filter(F.col("event_seq").between(6, 7))
    .select(
        "id",
        "budget",
        "prev_budget",
        F.col("prev_next_budget_int").cast("double").alias("prev_next_budget_int_str"),
    )
)

kept = (
    base
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")) & (F.col("c.is_remote")), "left_anti")
)

kept_keys = (
    kept
    .select("id")
    .intersectAll(
        df_departments
        .filter(~F.col("location").isin(["Tokyo", "Berlin", "New York"]))
        .select("id")
    )
)

reconciled = (
    kept
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .groupBy(
        F.when(F.col("budget") >= 3701000, "PREMIUM").otherwise("STANDARD").alias("budget_tier"),
    )
    .agg(
        F.round(F.avg("budget"), 2).alias("avg_budget"),
        F.max("prev_budget").alias("max_prev_budget"),
    )
    .orderBy(F.col("avg_budget").desc_nulls_first())
)
