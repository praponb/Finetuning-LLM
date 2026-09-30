from pyspark.sql import functions as F

src = (
    df_products
    .filter(F.size("tags") > 2)
    .withColumn("cost_rounded", F.round("cost", 0))
)

summary = (
    src
    .groupBy(
        F.when(F.year("launch_date") < 2024, "before_2024").when(F.year("launch_date") == 2024, "2024").otherwise("after_2024").alias("launch_cohort"),
    )
    .agg(
        F.max("launch_date").alias("last_launch_date"),
        F.count("*").alias("cnt"),
    )
)

result = (
    summary
    .filter(F.col("cnt") > 7)
    .orderBy("cnt")
)
