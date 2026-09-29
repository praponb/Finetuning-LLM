from pyspark.sql import functions as F

filtered_employees = (
    df_employees
    .filter(F.col("manager_id").isin([1, 24, 26, 40]) & F.col("level").isin(["L5", "L1"]))
)

summary = (
    filtered_employees
    .groupBy("is_remote", "level")
    .agg(
        F.sum(F.when(F.col("level") == "L5", 1).otherwise(0)).alias("l5_count"),
        F.max("salary").alias("max_salary"),
    )
)

result = (
    summary
    .select("is_remote", "l5_count", "max_salary")
    .orderBy(F.col("max_salary").desc(), F.col("l5_count").desc(), "is_remote")
)
