from pyspark.sql import functions as F

result = (
    df_employees
    .filter(~((F.col("manager_id") == 38) & F.col("is_remote")))
    .groupBy(
        F.when(F.col("level").isin(["L4", "L3"]), "priority").otherwise("other").alias("level_group"),
    )
    .agg(F.count(F.when(F.col("job_title") == "Engineer", 1)).alias("engineer_count"))
    .filter(F.col("engineer_count") > 3)
    .orderBy("level_group")
)
