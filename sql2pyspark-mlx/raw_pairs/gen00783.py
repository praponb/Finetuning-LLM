from pyspark.sql import functions as F

remaining_keys = (
    df_departments
    .select("location", "id")
    .intersectAll(
        df_departments
        .filter(F.col("dept_name") != "Marketing")
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
    .groupBy("dept_name")
    .agg(
        F.percentile_approx("budget", 0.5).alias("budget_p50"),
        F.stddev_samp("budget").alias("budget_stddev"),
    )
)
