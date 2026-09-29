from pyspark.sql import functions as F

filtered_stores = (
    df_stores
    .filter(F.col("region") == "North")
    .withColumn("is_central", F.when(F.col("region") == "Central", 1).otherwise(0))
)

agg = (
    filtered_stores
    .groupBy("region")
    .agg(
        F.sum("is_central").alias("total_is_central"),
        F.min("opened_date").alias("first_opened_date"),
    )
)

result = agg.filter(F.col("total_is_central") > 200)
