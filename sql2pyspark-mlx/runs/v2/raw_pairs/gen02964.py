from pyspark.sql import functions as F

result = (
    df_employees
    .filter(~F.col("is_remote") & F.col("manager_id").isNotNull() & ~F.col("full_name").like("Ivy%"))
    .groupBy("job_title")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("job_title"))).alias("job_title_values"),
        (F.max("salary") - F.min("salary")).alias("salary_range"),
    )
    .orderBy("job_title_values")
)
