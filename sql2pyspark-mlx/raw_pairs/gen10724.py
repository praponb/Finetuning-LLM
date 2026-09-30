from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_name").orderBy(F.col("budget").asc_nulls_first())
w2 = Window.partitionBy("dept_name").orderBy(F.col("budget_cume_dist").desc())
w3 = Window.partitionBy("dept_name", "location")
w4 = Window.partitionBy("budget_share_tier").orderBy(F.col("budget_p90").desc())

kept_keys = (
    df_departments
    .select("dept_name", "id")
    .subtract(
        df_departments
        .filter(F.col("dept_name").isin(["Finance", "Engineering"]))
        .select("dept_name", "id")
    )
)

reconciled = (
    df_departments
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.dept_name") == F.col("k.dept_name")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

matched_keys = (
    reconciled
    .select("dept_name")
    .subtract(
        df_departments
        .filter(F.col("dept_name") == "Marketing")
        .select("dept_name")
    )
)

kept_rows = (
    reconciled
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.dept_name") == F.col("k.dept_name")), "inner")
    .select("s.*")
)

enriched = (
    kept_rows
    .filter(F.col("location") == "Singapore")
    .select("*", F.cume_dist().over(w).alias("budget_cume_dist"))
)

scored = (
    enriched
    .select(
        "*",
        F.cume_dist().over(w2).alias("budget_cume_dist_cume_dist"),
        F.rank().over(w).alias("budget_rank"),
    )
)

scored_departments = (
    scored
    .select(
        "id",
        "budget",
        F.round((F.col("budget") / F.sum("budget").over(w3)), 4).alias("budget_share"),
    )
)

cleaned = (
    scored_departments
    .filter(F.col("budget") > 3876000)
    .withColumn("budget_per_budget_share", (F.col("budget") / F.nullif("budget_share", F.lit(0))))
)

result = (
    cleaned
    .groupBy(
        F.when(F.col("budget_share") >= 0.83, "HIGH").otherwise("LOW").alias("budget_share_tier"),
        F.when(F.col("budget_per_budget_share") >= 90, "HIGH").otherwise("LOW").alias("budget_per_budget_share_tier"),
    )
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.percentile_approx("budget", 0.9).alias("budget_p90"),
        F.count("*").alias("num_rows"),
    )
    .withColumn("budget_p90_rank", F.rank().over(w4))
)
