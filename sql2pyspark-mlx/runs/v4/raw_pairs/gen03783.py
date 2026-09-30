from pyspark.sql import functions as F

result = (
    df_departments
    .select("location", F.lit("department").alias("source"))
    .unionByName(
        df_stores
        .filter(F.col("sqft").between(9202, 12108))
        .select(F.col("city").alias("location"), F.lit("store").alias("source"))
    )
    .distinct()
    .orderBy("source")
)
