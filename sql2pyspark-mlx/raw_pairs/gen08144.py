from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("full_name").isNull() & (F.length("full_name") >= 12) & F.col("manager_id").isNotNull())
    .groupBy("level")
    .agg(
        F.max("salary").alias("max_salary"),
        F.concat_ws(", ", F.sort_array(F.collect_set("level"))).alias("level_values"),
        F.avg("salary").alias("avg_salary"),
        F.sum(F.when(F.col("level") == "L1", 1).otherwise(0)).alias("l1_count"),
    )
    .filter(F.col("l1_count") < 58)
    .drop("l1_count")
    .orderBy(F.col("avg_salary").desc())
)
