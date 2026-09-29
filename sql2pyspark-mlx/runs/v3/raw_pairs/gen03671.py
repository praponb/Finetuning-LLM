from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("name").like("%Lee%"))
    .groupBy(
        F.when(F.col("segment").isin(["Consumer", "SMB"]), "core").otherwise("other").alias("segment_group"),
    )
    .agg(
        F.countDistinct("credit_limit").alias("unique_countrys"),
        F.count(F.when(F.col("segment") == "Consumer", 1)).alias("consumer_count"),
    )
    .filter(F.col("consumer_count") >= 4)
    .orderBy("segment_group")
    .limit(3)
)
