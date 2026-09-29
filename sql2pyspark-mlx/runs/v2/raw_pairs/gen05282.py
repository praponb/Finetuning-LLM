from pyspark.sql import functions as F

result = (
    df_sessions
    .withColumn("country_group", F.when(F.col("country").isin(["DE", "TH"]), "Tier 1").otherwise("Other"))
    .orderBy("device")
)
