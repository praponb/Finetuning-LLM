from pyspark.sql import functions as F

result = (
    df_employees
    .filter((F.col("job_title") == "Analyst") & F.col("manager_id").isin([21, 33, 38]))
    .groupBy(
        F.when(F.col("level").isin(["L1", "L4"]), "group_a").otherwise("other").alias("level_group"),
    )
    .agg(
        F.round(((100 * F.sum(F.when(F.col("level") == "L2", 1).otherwise(0))) / F.count("*")), 1).alias("pct_l2"),
    )
    .orderBy(F.col("pct_l2").desc())
)
