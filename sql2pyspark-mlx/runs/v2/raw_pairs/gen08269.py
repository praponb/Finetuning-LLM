from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(F.col("country") != "JP")
    .withColumn("is_sg", F.when(F.col("country") == "SG", 1).otherwise(0))
    .orderBy("customer_id", "id")
    .limit(5)
)
