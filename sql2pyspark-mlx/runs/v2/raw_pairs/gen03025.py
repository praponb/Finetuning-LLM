from pyspark.sql import functions as F

result = (
    df_sessions
    .withColumn("browser_group", F.when(F.col("browser").isin(["Safari", "Edge"]), "Priority").otherwise("Other"))
    .withColumn("started_at_label", F.date_format("started_at", "yyyy-MM-dd"))
    .orderBy("started_at_label", "id")
    .limit(20)
)
