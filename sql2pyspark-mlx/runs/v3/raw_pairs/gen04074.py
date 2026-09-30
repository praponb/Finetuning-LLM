from pyspark.sql import functions as F

valid_customers = (
    df_customers
    .filter(~F.col("email").like("%@startup.io"))
    .withColumn("is_active_label", F.when(F.col("is_active"), "Yes").otherwise("No"))
)

agg = (
    valid_customers
    .groupBy("is_active")
    .agg(
        F.countDistinct("age").alias("unique_countrys"),
        F.round(((100 * F.sum(F.when(F.col("segment") == "Consumer", 1).otherwise(0))) / F.count("*")), 1).alias("pct_consumer"),
    )
)

result = (
    agg
    .filter(F.col("pct_consumer") < 72)
    .select("is_active", "unique_countrys", "pct_consumer")
    .orderBy(F.col("unique_countrys").desc())
)
