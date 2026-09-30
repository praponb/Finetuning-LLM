from pyspark.sql import functions as F

kept_keys = (
    df_departments
    .select("id")
    .subtract(
        df_departments
        .filter(F.col("budget") <= 2252000)
        .select("id")
    )
)

kept_rows = (
    df_departments
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .groupBy(
        F.when(F.col("budget") >= 3694000, "HIGH").otherwise("LOW").alias("budget_tier"),
        "location",
    )
    .agg(
        F.count(F.when(F.col("location") == "Tokyo", 1)).alias("tokyo_count"),
        F.stddev_samp("budget").alias("budget_stddev"),
    )
    .filter(F.col("budget_stddev") > 0)
)
