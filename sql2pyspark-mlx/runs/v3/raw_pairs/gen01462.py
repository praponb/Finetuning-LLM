from pyspark.sql import functions as F

src = (
    df_employees
    .filter((F.col("manager_id").isin([1, 18]) & (F.col("salary") <= 77000)) | F.col("email").like("%@GMAIL.COM"))
    .withColumn("email_filled", F.coalesce("email", F.lit("unassigned")))
)

agg = (
    src
    .groupBy("is_remote")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("job_title"))).alias("job_title_values"),
        F.round(F.avg("salary"), 2).alias("avg_salary"),
    )
)

result = (
    agg
    .filter(F.col("avg_salary") > 77)
    .select("is_remote", "job_title_values", "avg_salary")
    .orderBy("job_title_values")
)
