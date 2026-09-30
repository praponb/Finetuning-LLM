from pyspark.sql import functions as F

kept = (
    df_departments
    .alias("s")
    .join(df_employees.alias("x"), (F.col("x.dept_id") == F.col("s.id")) & (F.col("x.salary") > 91000), "left_anti")
)

cleaned = (
    kept
    .filter(F.col("budget").isNotNull())
    .select(
        "*",
        F.when(F.col("location").isin(["Singapore", "Bangkok"]), "Priority").otherwise("Other").alias("location_group"),
        F.coalesce("dept_name", F.lit("OTHER")).alias("dept_name_filled"),
    )
)

kept_keys = (
    cleaned
    .select("id", "location")
    .subtract(
        df_departments
        .filter(F.col("location") == "Berlin")
        .select("id", "location")
    )
)

reconciled = (
    cleaned
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.location") == F.col("k.location")), "inner")
    .select("s.*")
)

totals = (
    reconciled
    .groupBy(
        "dept_name",
        F.when(F.col("budget") >= 3803000, "LARGE").otherwise("SMALL").alias("budget_tier"),
    )
    .agg(
        F.count_if(F.col("location") == "Bangkok").alias("bangkok_events"),
        F.count_if(F.col("location") == "Chiang Mai").alias("chiang_mai_events"),
        F.countDistinct("location").alias("distinct_locations"),
        F.percentile_approx("budget", 0.5).alias("budget_p50"),
    )
)

result = (
    totals
    .select(
        "dept_name",
        "budget_tier",
        "chiang_mai_events",
        "distinct_locations",
        "budget_p50",
    )
)
