from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy(
        F.when(F.col("age") >= 72, "high").when(F.col("age") >= 24, "mid").otherwise("low").alias("age_bucket"),
    )
    .agg(F.countDistinct("age").alias("unique_countrys"))
    .filter(F.col("unique_countrys") < 8)
    .select("age_bucket", "unique_countrys")
)
