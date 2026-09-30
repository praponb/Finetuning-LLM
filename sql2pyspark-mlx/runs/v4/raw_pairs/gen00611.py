from pyspark.sql import functions as F

result = (
    df_employees
    .groupBy(
        "level",
        F.when(F.col("salary") >= 132000, "high").when(F.col("salary") >= 76000, "mid").otherwise("low").alias("salary_bucket"),
    )
    .agg(
        (F.max("salary") - F.min("salary")).alias("salary_range"),
        F.count(F.when(F.col("level") == "L2", 1)).alias("l2_count"),
    )
    .filter(F.col("salary_range") > 77)
    .orderBy(F.col("salary_range").desc())
)
