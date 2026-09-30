from pyspark.sql import functions as F

recent_employees = (
    df_employees
    .filter(F.col("manager_id") == 13)
    .withColumn("full_name_lower", F.lower("full_name"))
)

level_summary = (
    recent_employees
    .groupBy("level")
    .agg(
        F.sum(F.when(F.col("level") == "L1", F.col("salary")).otherwise(0)).alias("l1_salary"),
        F.count(F.when(F.col("level") == "L5", 1)).alias("l5_count"),
    )
)

result = (
    level_summary
    .filter(F.col("l5_count") < 5)
    .select("level", "l5_count")
    .orderBy("level", F.col("l5_count").desc())
    .limit(50)
)
