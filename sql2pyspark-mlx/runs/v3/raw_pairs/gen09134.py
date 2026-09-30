from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("dept_id").isin([19, 20]))
    .groupBy("job_title", "is_remote")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("level"))).alias("level_values"),
        F.sum(F.when(F.col("level") == "L2", F.col("salary")).otherwise(0)).alias("l2_salary"),
    )
    .filter(F.col("l2_salary") < 36)
    .select("job_title", "is_remote", "level_values", "l2_salary")
    .orderBy("l2_salary", "job_title", "is_remote")
    .limit(10)
)
