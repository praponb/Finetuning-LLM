from pyspark.sql import functions as F

kept_keys = (
    df_departments
    .select("dept_name", "id")
    .exceptAll(
        df_departments
        .filter(F.col("budget").between(1913000, 3284000))
        .select("dept_name", "id")
    )
)

reconciled = (
    df_departments
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.dept_name") == F.col("k.dept_name")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .withColumn("budget_tier", F.when(F.col("budget") >= 4465000, "PREMIUM").otherwise("STANDARD"))
    .groupingSets(
        [["budget_tier", "dept_name"], ["budget_tier"], []],
        "budget_tier",
        "dept_name",
    )
    .agg(
        F.count_if(F.col("location") == "New York").alias("new_york_events"),
        F.percentile_approx("budget", [0.25, 0.5, 0.75]).alias("budget_quartiles"),
        F.sum(F.when(F.col("dept_name") == "Legal", F.col("budget")).otherwise(0)).alias("legal_budget"),
        F.grouping("budget_tier").alias("gid"),
    )
    .filter(F.col("legal_budget") >= 0)
    .orderBy(F.col("budget_tier").asc_nulls_first())
)
