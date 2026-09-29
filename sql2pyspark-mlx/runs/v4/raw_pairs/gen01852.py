from pyspark.sql import functions as F

result = (
    df_departments
    .withColumn("budget_tier", F.when(F.col("budget") >= 4214000, "PREMIUM").otherwise("STANDARD"))
    .groupingSets(
        [["location", "budget_tier"], ["location"]],
        "location",
        "budget_tier",
    )
    .agg(
        F.sum(F.when(F.col("location") == "London", F.col("budget")).otherwise(0)).alias("london_budget"),
        F.count_if(F.col("location") == "Bangkok").alias("bangkok_events"),
        F.round(F.avg("budget"), 2).alias("avg_budget"),
        F.grouping_id().alias("grouping_level"),
    )
    .orderBy(F.col("avg_budget").desc_nulls_first())
)
