from pyspark.sql import functions as F

base = df_employees.filter(~F.col("is_remote"))

agg = (
    base
    .groupBy("level", "job_title")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("level") == "L1", 1).otherwise(0))) / F.count("*")), 1).alias("pct_l1"),
    )
)

result = (
    agg
    .filter(F.col("pct_l1") >= 61)
    .select("level", "job_title", "pct_l1")
)
