from pyspark.sql import functions as F

filtered_employees = (
    df_employees
    .filter(F.upper("email").like("%ENR%") & F.col("full_name").isNotNull() & F.col("hire_date").between("2024-04-14", "2025-07-26"))
    .withColumn("email_user", F.split(F.lower("email"), "@").getItem(0))
)

agg = (
    filtered_employees
    .groupBy(
        F.when(F.col("level").isin(["L5", "L1"]), "core").otherwise("other").alias("level_group"),
        "job_title",
    )
    .agg(F.countDistinct("salary").alias("unique_managers"))
)

result = agg.select("level_group", "job_title")
