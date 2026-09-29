from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_name").orderBy("budget_scaled", "id")
w2 = Window.partitionBy("dept_name", "location").orderBy("dedup_rank", "id").rowsBetween(-3, Window.currentRow)
w3 = Window.orderBy("budget", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("dept_name")

base = (
    df_departments
    .filter(F.col("dept_name").isNotNull() & (~F.col("location").eqNullSafe(F.lit("Tokyo"))))
    .select(
        "*",
        F.when(F.col("dept_name").isin(["Engineering", "Legal"]), "Core").otherwise("Other").alias("dept_name_group"),
        F.round((F.col("budget") * 0.85), 2).alias("budget_scaled"),
        F.coalesce("dept_name", F.lit("N/A")).alias("dept_name_filled"),
    )
)

ranked = (
    base
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
)

metrics = (
    ranked
    .filter(F.col("budget_scaled") >= 66)
    .select(
        "id",
        "dept_name",
        "budget",
        "budget_scaled",
        "dedup_rank",
        F.avg("budget").over(w2).alias("last4_avg_budget"),
        F.min("budget").over(w2).alias("last4_min_budget"),
        F.count("budget").over(w2).alias("last4_count_budget"),
        F.first("budget_scaled", ignorenulls=True).over(w3).alias("first_known_budget_scaled"),
    )
)

eligible = (
    metrics
    .alias("s")
    .join(df_employees.alias("ch"), (F.col("ch.dept_id") == F.col("s.id")) & (F.col("ch.manager_id").isin([19, 38])), "left_anti")
)

matched_keys = (
    eligible
    .select("dept_name", "id")
    .exceptAll(
        df_departments
        .filter(F.col("location") == "Berlin")
        .select("dept_name", "id")
    )
)

reconciled = (
    eligible
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.dept_name") == F.col("k.dept_name")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

with_windows = (
    reconciled
    .withColumn("budget_share", F.round((F.col("budget") / F.sum("budget").over(w4)), 4))
    .withColumn("dept_name_min_budget_scaled", F.min("budget_scaled").over(w4))
)

grouped = (
    with_windows
    .groupBy(
        F.when(F.col("last4_count_budget") >= 16, "HIGH").otherwise("LOW").alias("last4_count_budget_tier"),
        F.when(F.col("dept_name_min_budget_scaled") >= 60, "HIGH").otherwise("LOW").alias("dept_name_min_budget_scaled_tier"),
    )
    .agg(
        F.percentile_approx("budget_share", 0.9).alias("budget_share_p90"),
        F.count(F.when(F.col("dept_name") == "Marketing", 1)).alias("marketing_rows"),
    )
    .filter(F.col("budget_share_p90") < 1000000)
)

result = (
    grouped
    .select(
        "last4_count_budget_tier",
        "dept_name_min_budget_scaled_tier",
        "budget_share_p90",
    )
    .orderBy(F.col("dept_name_min_budget_scaled_tier").desc_nulls_last())
)
