from pyspark.sql import functions as F

result = (
    df_employees
    .groupBy("level")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("level") == "L1", 1).otherwise(0))) / F.count("*")), 1).alias("pct_l1"),
        F.count(F.when(F.col("level") == "L1", 1)).alias("l1_count"),
        F.max("salary").alias("max_salary"),
    )
)
