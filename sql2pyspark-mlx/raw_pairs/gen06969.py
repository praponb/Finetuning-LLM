from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_departments.alias("d"), F.col("e.dept_id") == F.col("d.id"), "inner")
    .filter(F.upper(F.trim(F.col("e.full_name"))) == "OLIVIA GARCIA")
    .groupBy(
        F.when(F.col("d.location").isin(["Tokyo", "London"]), "tier_1").otherwise("other").alias("location_group"),
    )
    .agg(F.count(F.when(F.col("e.level") == "L2", 1)).alias("l2_count"))
)
