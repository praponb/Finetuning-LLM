from pyspark.sql import functions as F

result = (
    df_employees
    .agg(
        F.round(((100 * F.sum(F.when(F.col("level") == "L2", 1).otherwise(0))) / F.count("*")), 1).alias("pct_l2"),
        F.max("salary").alias("max_salary"),
    )
)
