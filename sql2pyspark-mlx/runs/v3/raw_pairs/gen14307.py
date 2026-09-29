from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("dept_id").isin([1, 22, 33]))
    .groupBy(
        "is_remote",
        F.when(F.col("salary") >= 146000, "high").when(F.col("salary") >= 64000, "mid").otherwise("low").alias("salary_bucket"),
    )
    .agg(F.count(F.when(F.col("level") == "L1", 1)).alias("l1_count"))
    .filter(F.col("l1_count") > 8)
    .select("is_remote", "salary_bucket", "l1_count")
    .orderBy("salary_bucket", "l1_count", "is_remote")
    .limit(5)
)
