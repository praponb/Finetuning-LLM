from pyspark.sql import functions as F

matched_keys = (
    df_employees
    .select("manager_id", "id")
    .subtract(
        df_employees
        .filter(F.col("dept_id").isin([3, 8, 13, 36]))
        .select("manager_id", "id")
    )
)

verified = (
    df_employees
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.manager_id") == F.col("k.manager_id")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    verified
    .withColumn("salary_tier", F.when(F.col("salary") >= 139000, "PREMIUM").otherwise("STANDARD"))
    .groupingSets([["level", "salary_tier"], ["level"], []], "level", "salary_tier")
    .agg(
        F.count(F.when(F.col("level") == "L5", 1)).alias("l5_rows"),
        F.avg(F.when(F.col("level") == "L5", F.col("salary"))).alias("l5_avg_salary"),
        F.percentile_approx("salary", 0.5).alias("salary_p50"),
        F.grouping("level").alias("grouping_level"),
    )
    .filter(F.col("salary_p50") > 0)
)
