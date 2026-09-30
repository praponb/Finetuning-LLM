from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("is_remote"))
    .groupBy("level")
    .agg(F.count(F.when(F.col("level") == "L3", 1)).alias("l3_count"))
    .filter(F.col("l3_count") < 7)
    .select("level", "l3_count")
    .orderBy(F.col("l3_count").desc(), "level")
    .limit(3)
)
