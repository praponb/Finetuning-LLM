from pyspark.sql import functions as F

remaining_keys = (
    df_employees
    .select("level", "manager_id")
    .intersect(
        df_employees
        .filter(F.col("full_name").like("%Tay%"))
        .select("level", "manager_id")
    )
)

verified = (
    df_employees
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.level") == F.col("k.level")) & (F.col("s.manager_id") == F.col("k.manager_id")), "inner")
    .select("s.*")
)

result = (
    verified
    .groupBy("level")
    .agg(
        F.count(F.when(F.col("job_title") == "Designer", 1)).alias("designer_count"),
        F.count(F.when(F.col("job_title") == "Designer", 1)).alias("designer_rows"),
        F.countDistinct("level").alias("distinct_levels"),
    )
    .orderBy(F.col("designer_count").asc_nulls_first())
)
