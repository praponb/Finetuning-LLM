from pyspark.sql import functions as F

result = (
    df_employees
    .groupBy("level")
    .agg(
        F.count(F.when(F.col("level") == "L3", 1)).alias("l3_count"),
        F.count(F.when(F.col("level") == "L5", 1)).alias("l5_count"),
    )
    .filter(F.col("l3_count") >= 3)
    .orderBy(F.col("l3_count").desc())
)
