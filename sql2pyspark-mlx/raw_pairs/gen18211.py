from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy(
        F.when(F.col("location").isin(["New York", "Chiang Mai"]), "tier_1").otherwise("other").alias("location_group"),
    )
    .agg(
        F.sum(F.when(F.col("dept_name") == "Marketing", 1).otherwise(0)).alias("marketing_count"),
    )
    .orderBy("marketing_count", "location_group")
    .limit(1)
)
