from pyspark.sql import functions as F

kept_keys = (
    df_departments
    .select("dept_name", "id")
    .subtract(
        df_departments
        .filter(F.col("location") == "Bangkok")
        .select("dept_name", "id")
    )
)

verified = (
    df_departments
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.dept_name") == F.col("k.dept_name")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    verified
    .groupBy("dept_name")
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.round(F.sum("budget") / F.nullif(F.count(F.when(F.col("location") == "Chiang Mai", 1)), F.lit(0)), 2).alias("budget_per_chiang_mai"),
        F.stddev_samp("budget").alias("budget_stddev"),
        F.round(F.sum("budget") / F.nullif(F.count(F.when(F.col("dept_name") == "HR", 1)), F.lit(0)), 2).alias("budget_per_hr"),
        F.count("*").alias("num_rows"),
        F.count_if(F.col("location") == "Bangkok").alias("bangkok_events"),
    )
    .filter(F.col("bangkok_events") > 1)
    .orderBy(F.col("budget_per_hr").desc())
)
