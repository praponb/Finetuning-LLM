from pyspark.sql import functions as F

valid_suppliers = (
    df_suppliers
    .filter(F.col("supplier_name").like("Eu%") & (F.col("rating") > 1.8))
    .withColumn("is_sg", F.when(F.col("country") == "SG", 1).otherwise(0))
)

summary = (
    valid_suppliers
    .groupBy(
        F.when(F.col("is_sg") >= 65, "high").when(F.col("is_sg") >= 24, "mid").otherwise("low").alias("is_sg_bucket"),
    )
    .agg(F.avg("rating").alias("avg_rating"))
)

result = (
    summary
    .filter(F.col("avg_rating") < 2.8)
    .select("is_sg_bucket", "avg_rating")
    .orderBy("is_sg_bucket", F.col("avg_rating").desc())
    .limit(10)
)
