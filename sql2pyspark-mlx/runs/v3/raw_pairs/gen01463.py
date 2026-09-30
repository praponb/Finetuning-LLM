from pyspark.sql import functions as F

result = (
    df_employees
    .groupBy("is_remote")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("job_title"))).alias("job_title_values"),
        F.sum(F.when(F.col("level") == "L4", F.col("salary")).otherwise(0)).alias("l4_salary"),
    )
    .filter(F.col("l4_salary") >= 26)
    .select("is_remote", "job_title_values", "l4_salary")
    .orderBy(F.col("l4_salary").desc(), "is_remote")
    .limit(10)
)
