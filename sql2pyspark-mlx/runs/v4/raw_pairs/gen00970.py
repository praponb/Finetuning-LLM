from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("salary") >= 104000)
    .agg(
        F.round(((100 * F.sum(F.when(F.col("job_title") == "Designer", 1).otherwise(0))) / F.count("*")), 1).alias("pct_designer"),
        F.round(((100 * F.sum(F.when(F.col("level") == "L1", 1).otherwise(0))) / F.count("*")), 1).alias("pct_l1"),
    )
    .orderBy(F.col("pct_l1").desc(), F.col("pct_designer").desc())
)
