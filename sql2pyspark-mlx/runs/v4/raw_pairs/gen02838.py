from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("location").orderBy("budget_filled", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy("budget_filled", "id")
w3 = Window.orderBy("budget_filled", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("dept_name").orderBy(F.col("budget_filled").desc(), "id")
w5 = Window.partitionBy("location", "dept_name").orderBy("cumulative_budget_filled", "id")
w6 = Window.partitionBy("location", "dept_name").orderBy(F.col("budget_filled_plus_budget").desc(), "id")

valid = (
    df_departments
    .filter(F.col("location").isin(["Chiang Mai", "London", "Tokyo"]))
    .withColumn("budget_filled", F.coalesce("budget", F.lit(0)))
)

filtered = (
    valid
    .filter(F.col("location").isin(["Bangkok", "London", "Chiang Mai"]))
    .select(
        "id",
        "dept_name",
        "location",
        "budget_filled",
        (F.col("budget_filled") + F.col("budget")).alias("budget_filled_plus_budget"),
        F.coalesce("location", F.lit("UNKNOWN")).alias("resolved_location"),
    )
)

metrics = (
    filtered
    .select(
        "*",
        F.sum("budget_filled").over(w).alias("cumulative_budget_filled"),
        F.lead("budget_filled", 1, 0).over(w2).alias("next_budget_filled"),
        F.sum("budget_filled").over(w3).alias("running_budget_filled"),
    )
)

enriched = (
    metrics
    .select(
        "*",
        F.ntile(5).over(w4).alias("budget_filled_bucket"),
        F.last("budget_filled", ignorenulls=True).over(w).alias("last_known_budget_filled"),
        F.lead("budget_filled_plus_budget", 1).over(w5).alias("next_budget_filled_plus_budget"),
        F.row_number().over(w6).alias("row_seq"),
    )
)

cleaned = (
    enriched
    .filter(F.col("location").isNotNull())
    .withColumn("running_budget_filled_scaled", F.round((F.col("running_budget_filled") * 1.07), 2))
    .withColumn("budget_filled_plus_budget_per_next_budget_filled", (F.col("budget_filled_plus_budget") / F.nullif("next_budget_filled", F.lit(0))))
    .withColumn("resolved_location_2", F.coalesce("location", F.lit("NONE")))
)

result = (
    cleaned
    .groupBy("dept_name", "location")
    .agg(
        F.count("*").alias("num_rows"),
        F.percentile_approx("next_budget_filled", 0.5).alias("next_budget_filled_p50"),
        F.size(F.collect_set("location")).alias("num_locations"),
        F.round(F.sum("budget_filled_plus_budget") / F.nullif(F.count(F.when(F.col("dept_name") == "Finance", 1)), F.lit(0)), 2).alias("budget_filled_plus_budget_per_finance"),
        F.count("*").alias("row_count"),
        F.round(F.avg("cumulative_budget_filled"), 2).alias("avg_cumulative_budget_filled"),
    )
    .filter(F.col("next_budget_filled_p50") >= 0)
    .orderBy(F.col("row_count").asc_nulls_first())
)
