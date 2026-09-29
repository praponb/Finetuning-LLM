from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("location").orderBy(F.col("least_budget_budget_rounded").desc(), "id")

cleaned = (
    df_departments
    .filter(F.col("location").isNotNull())
    .select(
        "*",
        F.initcap(F.trim("location")).alias("location_title"),
        F.coalesce("location", F.lit("DIRECT")).alias("location_filled"),
    )
)

filtered = (
    cleaned
    .filter(F.col("dept_name").isNotNull())
    .withColumn("budget_rounded", F.round("budget", 1))
)

filtered_departments = (
    filtered
    .filter(F.col("dept_name").isNotNull())
    .select(
        "*",
        F.least("budget", "budget_rounded").alias("least_budget_budget_rounded"),
        F.concat_ws("-", "location", "dept_name").alias("location_dept_name_key"),
        F.coalesce("dept_name", F.lit("DIRECT")).alias("resolved_dept_name"),
    )
)

latest = (
    filtered_departments
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
)

cleaned_departments = (
    latest
    .filter(~F.col("dept_name").isin(["Legal", "Marketing"]))
    .select(
        "id",
        "dept_name",
        "location",
        "budget",
        "location_title",
        "least_budget_budget_rounded",
        (F.col("budget_rounded") + F.col("least_budget_budget_rounded")).alias("budget_rounded_plus_least_budget_budget_rounded"),
        F.coalesce("budget", F.lit(0)).alias("budget_filled"),
        F.coalesce("location", F.lit("UNKNOWN")).alias("resolved_location"),
    )
)

stats = (
    cleaned_departments
    .groupBy(
        F.when(F.col("least_budget_budget_rounded") >= 83, "PREMIUM").otherwise("STANDARD").alias("least_budget_budget_rounded_tier"),
    )
    .agg(
        F.round(F.sum("budget_filled") / F.nullif(F.count(F.when(F.col("dept_name") == "Legal", 1)), F.lit(0)), 2).alias("budget_filled_per_legal"),
        F.percentile_approx("budget_filled", [0.25, 0.5, 0.75]).alias("budget_filled_quartiles"),
        F.round(F.sum("budget_filled") / F.nullif(F.count(F.when(F.col("dept_name") == "Marketing", 1)), F.lit(0)), 2).alias("budget_filled_per_marketing"),
    )
    .filter(F.col("budget_filled_per_legal") > 0)
)

result = (
    stats
    .select(
        "least_budget_budget_rounded_tier",
        "budget_filled_per_legal",
        "budget_filled_per_marketing",
    )
)
