from pyspark.sql import functions as F

cleaned = (
    df_employees
    .filter((F.col("level").eqNullSafe(F.lit("L5"))) & (F.col("hire_date") >= F.to_date(F.lit("2023-08-15"))))
    .select(
        "*",
        F.floor("salary").alias("salary_whole"),
        F.coalesce("full_name", F.lit("N/A")).alias("full_name_filled"),
    )
)

result = (
    cleaned
    .rollup(
        F.when(F.col("salary") >= 148000, "HIGH").otherwise("LOW").alias("salary_tier"),
    )
    .agg(
        F.count(F.when(F.col("job_title") == "Engineer", 1)).alias("engineer_count"),
        F.count_if(F.col("level") == "L5").alias("l5_events"),
        F.percentile_approx("salary_whole", 0.9).alias("salary_whole_p90"),
        F.size(F.collect_set("level")).alias("num_levels"),
    )
    .orderBy(F.col("salary_tier").desc_nulls_first())
)
