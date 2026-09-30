from pyspark.sql import functions as F

valid = (
    df_employees
    .filter((F.col("hire_date") > F.lit("2023-09-12").cast("date")) & F.col("manager_id").isNotNull())
    .withColumn("level_lower", F.lower("level"))
    .withColumn("job_title_title", F.initcap(F.trim("job_title")))
    .withColumn("level_filled", F.coalesce("level", F.lit("DIRECT")))
)

result = (
    valid
    .groupBy("manager_id")
    .pivot("level", ["L1", "L5", "L3"])
    .agg(F.max("salary"))
    .select(
        "manager_id",
        F.col("L1").alias("l1"),
        F.col("L5").alias("l5"),
        F.col("L3").alias("l3"),
    )
)
