from pyspark.sql import functions as F

kept_keys = (
    df_employees
    .select("id")
    .intersectAll(
        df_employees
        .filter(F.col("manager_id").isNotNull())
        .select("id")
    )
)

verified = (
    df_employees
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    verified
    .groupBy(
        F.when(F.col("salary") >= 128000, "HIGH").otherwise("LOW").alias("salary_tier"),
        F.date_trunc("MONTH", "hire_date").alias("activity_month"),
    )
    .agg(
        F.percentile_approx("salary", 0.9).alias("salary_p90"),
        F.size(F.collect_set("level")).alias("num_levels"),
        F.percentile_approx("salary", [0.25, 0.5, 0.75]).alias("salary_quartiles"),
    )
    .orderBy(F.col("activity_month").desc_nulls_first())
)
