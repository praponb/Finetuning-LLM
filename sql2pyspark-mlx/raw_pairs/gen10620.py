from pyspark.sql import functions as F

remaining_keys = (
    df_departments
    .select("location", "id")
    .intersect(
        df_departments
        .filter(F.col("location").isin(["Chiang Mai", "Bangkok", "Tokyo"]))
        .select("location", "id")
    )
)

verified = (
    df_departments
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.location") == F.col("k.location")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    verified
    .withColumn("budget_tier", F.when(F.col("budget") >= 4260000, "PREMIUM").otherwise("STANDARD"))
    .cube("budget_tier", "location")
    .agg(
        F.round(F.sum("budget") / F.nullif(F.count(F.when(F.col("location") == "Bangkok", 1)), F.lit(0)), 2).alias("budget_per_bangkok"),
        F.round(F.avg("budget"), 2).alias("avg_budget"),
        F.sort_array(F.collect_set("dept_name")).alias("dept_name_list"),
        F.stddev_samp("budget").alias("budget_stddev"),
        F.grouping("budget_tier").alias("gid"),
    )
    .orderBy(F.col("budget_stddev").desc())
)
