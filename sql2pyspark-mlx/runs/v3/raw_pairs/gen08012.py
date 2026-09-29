from pyspark.sql import functions as F

base = (
    df_employees
    .filter(~F.col("full_name").like("%Oli%") & (F.col("manager_id") == 34))
    .withColumn("full_name_prefix", F.substring("full_name", 1, 2))
)

agg = (
    base
    .groupBy(
        F.when(F.year("hire_date") < 2024, "before_2024").when(F.year("hire_date") == 2024, "2024").otherwise("after_2024").alias("hire_cohort"),
    )
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("job_title"))).alias("job_title_values"),
    )
)

result = (
    agg
    .select("hire_cohort", "job_title_values")
    .orderBy("hire_cohort")
    .limit(50)
)
