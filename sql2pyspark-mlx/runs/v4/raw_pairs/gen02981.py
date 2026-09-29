from pyspark.sql import functions as F

recent_employees = (
    df_employees
    .filter((F.col("salary") <= 96000) & (F.col("level") == "L5") & F.col("level").isin(["L5", "L4"]))
    .withColumn("job_title_filled", F.coalesce("job_title", F.lit("Unknown")))
)

is_remote_summary = (
    recent_employees
    .groupBy("is_remote")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("level"))).alias("level_values"),
        F.max("hire_date").alias("latest_hire_date"),
    )
)

result = is_remote_summary
