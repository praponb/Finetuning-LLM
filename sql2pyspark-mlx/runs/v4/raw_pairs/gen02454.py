from pyspark.sql import functions as F

filtered = (
    df_employees
    .filter((F.col("hire_date") > F.lit("2023-12-08").cast("date")) & (F.length("full_name") >= 8))
    .select(
        "*",
        F.concat_ws("-", "job_title", "email").alias("job_title_email_key"),
        F.coalesce("job_title", F.lit("N/A")).alias("resolved_job_title"),
    )
)

result = (
    filtered
    .groupBy("dept_id")
    .pivot("level", ["L3", "L1"])
    .agg(F.max("salary").alias("total"), F.count(F.lit(1)).alias("cnt"))
    .select(
        "dept_id",
        F.col("L3_total").alias("l3_total"),
        F.col("L3_cnt").alias("l3_cnt"),
        F.col("L1_total").alias("l1_total"),
        F.col("L1_cnt").alias("l1_cnt"),
    )
)
