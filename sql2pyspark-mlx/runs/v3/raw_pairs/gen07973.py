from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("manager_id").isNotNull())
    .groupBy("is_remote")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("level") == "L1", 1).otherwise(0))) / F.count("*")), 1).alias("pct_l1"),
    )
)
