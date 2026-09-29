from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.partitionBy("dept_name").orderBy("budget_adjusted", "id")
w2 = Window.partitionBy("dept_name", "location")
w3 = Window.partitionBy("location")

valid = (
    df_departments
    .filter(F.col("location") == "Chiang Mai")
    .select(
        "*",
        F.when(F.col("budget") >= 3429000, "High").when(F.col("budget") >= 881000, "Medium").otherwise("Low").alias("budget_tier"),
        F.round((F.col("budget") * 100), 2).alias("budget_adjusted"),
    )
)

metrics = (
    valid
    .select(
        "id",
        "location",
        "budget_tier",
        "budget_adjusted",
        F.lead("budget").over(w_running).alias("next_budget"),
        F.lead("budget_adjusted").over(w_running).alias("next_budget_adjusted"),
        F.lag("budget_adjusted").over(w_running).alias("prev_budget_adjusted"),
        F.row_number().over(w_running).alias("row_seq"),
        F.min("budget_adjusted").over(w2).alias("dept_name_min_budget_adjusted"),
    )
)

base = (
    metrics
    .filter(F.col("location").isNotNull() & (F.col("next_budget") <= 3042000))
    .select(
        "id",
        "location",
        "budget_tier",
        "budget_adjusted",
        "next_budget_adjusted",
        "prev_budget_adjusted",
        "row_seq",
        F.substring("location", 1, 2).alias("location_prefix"),
        F.greatest("dept_name_min_budget_adjusted", "prev_budget_adjusted").alias("greatest_dept_name_min_budget_adjusted_prev_budget_adjusted"),
        F.coalesce("location", F.lit("N/A")).alias("resolved_location"),
    )
)

kept_keys = (
    base
    .select("id", "location")
    .subtract(
        df_departments
        .filter(F.col("budget").between(2630000, 2760000))
        .select("id", "location")
    )
)

verified = (
    base
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.location") == F.col("k.location")), "inner")
    .select("s.*")
)

enriched = verified.withColumn("location_avg_row_seq", F.avg("row_seq").over(w3))

cleaned = (
    enriched
    .filter(F.col("location").isNotNull() & (~F.col("location").eqNullSafe(F.lit("London"))))
    .select(
        "location",
        "next_budget_adjusted",
        "row_seq",
        "greatest_dept_name_min_budget_adjusted_prev_budget_adjusted",
        "resolved_location",
        "location_avg_row_seq",
        F.floor("greatest_dept_name_min_budget_adjusted_prev_budget_adjusted").alias("greatest_dept_name_min_budget_adjusted_prev_budget_adjusted_whole"),
    )
)

result = (
    cleaned
    .select(
        "location",
        "next_budget_adjusted",
        "row_seq",
        "greatest_dept_name_min_budget_adjusted_prev_budget_adjusted_whole",
        F.when(F.col("location").rlike("^(London|Chiang Mai)$"), "GROUP_B").when(F.col("location").rlike("^(Bangkok|New York)$"), "PRIORITY").otherwise("EXTENDED").alias("location_zone"),
    )
)
