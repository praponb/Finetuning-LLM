from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("location", "dept_name")
w2 = Window.partitionBy("location").orderBy(F.col("adjusted_budget").asc_nulls_first())
w3 = Window.partitionBy("location")

filtered = (
    df_departments
    .filter(F.col("location").isNotNull() & (~F.col("location").eqNullSafe(F.lit("Bangkok"))))
    .withColumn("adjusted_budget", F.round((F.col("budget") * 100), 2))
    .withColumn("dept_name_location", F.concat("dept_name", F.lit("/"), "location"))
    .withColumn("location_filled", F.coalesce("location", F.lit("N/A")))
)

valid = (
    filtered
    .filter(F.col("budget") > 3108000)
    .select(
        "*",
        (F.col("budget") - F.col("adjusted_budget")).alias("budget_minus_adjusted_budget"),
        F.concat_ws("-", "dept_name", "location").alias("dept_name_location_key"),
    )
)

matched_keys = (
    valid
    .select("id")
    .exceptAll(
        df_departments
        .filter(~F.col("dept_name").isin(["HR", "Finance", "Sales"]))
        .select("id")
    )
)

verified = (
    valid
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

not_flagged = (
    verified
    .alias("s")
    .join(df_employees.alias("x"), (F.col("x.dept_id") == F.col("s.id")) & (F.col("x.email").like("%@company.co")), "left_semi")
)

enriched = (
    not_flagged
    .filter(F.col("adjusted_budget").between(52, 75))
    .select(
        "*",
        F.max("budget").over(w).alias("location_max_budget"),
        F.percent_rank().over(w2).alias("adjusted_budget_percent_rank"),
        F.sum("budget_minus_adjusted_budget").over(w3).alias("location_sum_budget_minus_adjusted_budget"),
    )
)

base = (
    enriched
    .filter(F.col("adjusted_budget") <= 58)
    .select(
        "id",
        "dept_name",
        "location",
        "budget",
        "adjusted_budget",
        "dept_name_location",
        "budget_minus_adjusted_budget",
        "location_max_budget",
        "adjusted_budget_percent_rank",
        "location_sum_budget_minus_adjusted_budget",
        (F.col("budget") + F.col("adjusted_budget_percent_rank")).alias("budget_plus_adjusted_budget_percent_rank"),
        F.coalesce("dept_name", F.lit("DIRECT")).alias("resolved_dept_name"),
    )
)

not_flagged_departments = (
    base
    .alias("s")
    .join(df_employees.alias("ch"), (F.col("ch.dept_id") == F.col("s.id")) & (F.col("ch.job_title").isNull()), "left_anti")
)

stats = (
    not_flagged_departments
    .withColumn("budget_minus_adjusted_budget_tier", F.when(F.col("budget_minus_adjusted_budget") >= 82, "PREMIUM").otherwise("STANDARD"))
    .groupingSets(
        [["dept_name", "budget_minus_adjusted_budget_tier", "location"], ["dept_name", "budget_minus_adjusted_budget_tier"]],
        "dept_name",
        "budget_minus_adjusted_budget_tier",
        "location",
    )
    .agg(
        F.sort_array(F.collect_set("dept_name")).alias("dept_name_list"),
        F.count_if(F.col("dept_name") == "HR").alias("hr_events"),
        F.percentile_approx("budget_plus_adjusted_budget_percent_rank", [0.25, 0.5, 0.75]).alias("budget_plus_adjusted_budget_percent_rank_quartiles"),
        F.grouping_id().alias("grouping_id"),
    )
    .filter(F.col("hr_events") >= 1)
)

result = (
    stats
    .select(
        "budget_minus_adjusted_budget_tier",
        "location",
        "hr_events",
        "budget_plus_adjusted_budget_percent_rank_quartiles",
        "grouping_id",
    )
    .orderBy(F.col("grouping_id").desc_nulls_first())
)
