from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("country").isin(["UK", "SG", "JP"]))
    .groupBy("country")
    .agg(
        F.sum(F.when(F.col("country") == "SG", 1).otherwise(0)).alias("sg_count"),
        F.max("rating").alias("max_rating"),
    )
    .filter(F.col("max_rating") < 3.7)
    .orderBy(F.col("sg_count").desc(), "country")
    .limit(10)
)
