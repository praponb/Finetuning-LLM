from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.length("email") < 14)
    .groupBy("is_remote", "dept_id")
    .agg(
        F.count(F.when(F.col("level") == "L2", 1)).alias("l2_count"),
        F.min("hire_date").alias("min_hire_date"),
    )
    .filter(F.col("l2_count") < 7)
    .select("is_remote", "dept_id", "l2_count", "min_hire_date")
)
