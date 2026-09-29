from pyspark.sql import functions as F

base = (
    df_employees
    .filter(F.col("manager_id").isNull() & F.col("level").isin(["L3", "L4", "L1"]))
    .withColumn("is_remote_flag", F.col("is_remote").cast("int"))
)

agg = (
    base
    .groupBy("job_title")
    .agg(
        F.sum(F.when(F.col("job_title") == "Designer", F.col("is_remote_flag")).otherwise(0)).alias("designer_is_remote_flag"),
        (F.max("salary") - F.min("salary")).alias("salary_range"),
    )
)

result = agg
