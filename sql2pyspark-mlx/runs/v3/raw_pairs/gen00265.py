from pyspark.sql import functions as F

src = df_employees.filter((F.length("email") > 13) & F.col("job_title").isNull())

level_summary = (
    src
    .groupBy("level")
    .agg(
        F.sum(F.when(F.col("job_title") == "Director", 1).otherwise(0)).alias("director_count"),
        F.count(F.when(F.col("level") == "L3", 1)).alias("l3_count"),
    )
)

result = (
    level_summary
    .select("level", "director_count")
    .orderBy("level")
    .limit(5)
)
