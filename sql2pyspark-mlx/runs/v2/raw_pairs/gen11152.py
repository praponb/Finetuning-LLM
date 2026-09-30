from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("full_name").isNotNull())
    .groupBy(F.quarter("hire_date").alias("hire_quarter"), "level")
    .agg(
        F.count("*").alias("total_count"),
        F.sum(F.when(F.col("level") == "L1", 1).otherwise(0)).alias("l1_count"),
    )
    .filter(F.col("total_count") > 7)
    .select("hire_quarter", "level", "total_count", "l1_count")
    .orderBy(F.col("l1_count").desc())
)
