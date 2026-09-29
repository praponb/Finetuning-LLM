from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("level").isin(["L2", "L5"]) & F.col("full_name").isNotNull() & F.col("dept_id").isin([21, 40]))
    .groupBy(
        F.when(F.year("hire_date") < 2024, "before_2024").when(F.year("hire_date") == 2024, "2024").otherwise("after_2024").alias("hire_cohort"),
        "job_title",
    )
    .agg(
        F.sort_array(F.collect_set("level")).alias("level_list"),
        F.sum(F.when(F.col("job_title") == "Designer", 1).otherwise(0)).alias("designer_count"),
    )
    .orderBy(F.col("designer_count").desc())
)
