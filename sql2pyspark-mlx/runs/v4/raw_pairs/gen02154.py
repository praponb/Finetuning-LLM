from pyspark.sql import functions as F

matched_keys = (
    df_departments
    .select("location", "dept_name")
    .exceptAll(
        df_departments
        .filter(F.col("dept_name") == "HR")
        .select("location", "dept_name")
    )
)

reconciled = (
    df_departments
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.location") == F.col("k.location")) & (F.col("s.dept_name") == F.col("k.dept_name")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .groupBy(
        F.when(F.col("budget") >= 3511000, "LARGE").otherwise("SMALL").alias("budget_tier"),
        "dept_name",
    )
    .agg(
        F.percentile_approx("budget", [0.25, 0.5, 0.75]).alias("budget_quartiles"),
        F.size(F.collect_set("dept_name")).alias("num_dept_names"),
    )
    .filter(F.col("num_dept_names") < 1)
)
