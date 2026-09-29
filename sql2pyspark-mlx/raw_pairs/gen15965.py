from pyspark.sql import functions as F

cleaned = (
    df_employees
    .filter(F.length("full_name") < 6)
    .select(
        "*",
        F.when(F.col("is_remote"), "Yes").otherwise("No").alias("is_remote_label"),
        F.add_months("hire_date", 3).alias("hire_date_plus_months"),
    )
)

result = (
    cleaned
    .groupBy("level")
    .pivot("job_title", ["Director", "Engineer"])
    .agg(F.sum("salary").alias("value_sum"), F.count(F.lit(1)).alias("n"))
)
