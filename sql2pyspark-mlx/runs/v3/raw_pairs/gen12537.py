from pyspark.sql import functions as F

valid_departments = (
    df_departments
    .filter(F.col("location").isNull() & (F.col("budget") < 2503000))
    .withColumn("dept_name_prefix", F.substring("dept_name", 1, 4))
)

dept_name_group_summary = (
    valid_departments
    .groupBy(
        F.when(F.col("dept_name").isin(["Marketing", "Finance"]), "tier_1").otherwise("other").alias("dept_name_group"),
    )
    .agg(F.count("*").alias("row_count"))
)

result = dept_name_group_summary.select("dept_name_group", "row_count")
