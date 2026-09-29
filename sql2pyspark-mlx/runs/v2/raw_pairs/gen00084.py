from pyspark.sql import functions as F

recent_employees = (
    df_employees
    .filter(~F.col("is_remote"))
    .withColumn("full_name_title", F.initcap(F.trim("full_name")))
)

summary = (
    recent_employees
    .groupBy(F.date_format("hire_date", "yyyy-MM").alias("hire_month"))
    .agg(
        F.sum(F.when(F.col("level") == "L1", 1).otherwise(0)).alias("l1_count"),
        (F.max("salary") - F.min("salary")).alias("salary_range"),
    )
)

result = summary
