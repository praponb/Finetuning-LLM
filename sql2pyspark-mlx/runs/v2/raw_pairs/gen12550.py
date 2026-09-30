from pyspark.sql import functions as F

filtered_employees = (
    df_employees
    .filter((F.col("level") == "L3") & F.col("email").like("%@yahoo.com") & F.col("manager_id").isNotNull())
    .withColumn("job_title_lower", F.lower("job_title"))
)

is_remote_summary = (
    filtered_employees
    .groupBy("is_remote")
    .agg(
        F.sum(F.when(F.col("level") == "L2", 1).otherwise(0)).alias("l2_count"),
        F.concat_ws(", ", F.sort_array(F.collect_set("level"))).alias("level_values"),
    )
)

result = is_remote_summary.orderBy(F.col("level_values").desc())
