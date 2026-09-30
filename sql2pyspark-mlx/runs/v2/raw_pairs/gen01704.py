from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.upper(F.trim("email")) == "LIAM@OUTLOOK.COM")
    .groupBy(
        "segment",
        F.when(F.col("age") >= 58, "high").when(F.col("age") >= 42, "mid").otherwise("low").alias("age_bucket"),
    )
    .agg(F.countDistinct("age").alias("unique_segments"))
    .filter(F.col("unique_segments") > 7)
    .select("segment", "age_bucket", "unique_segments")
    .orderBy("unique_segments", "segment", "age_bucket")
    .limit(1)
)
