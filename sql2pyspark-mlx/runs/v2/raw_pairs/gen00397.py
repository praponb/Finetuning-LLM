from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("end_date").between("2024-08-04", "2025-08-16"))
    .withColumn("status_group", F.when(F.col("status").isin(["ACTIVE", "EXPIRED"]), "Group A").otherwise("Other"))
    .orderBy("plan")
)
