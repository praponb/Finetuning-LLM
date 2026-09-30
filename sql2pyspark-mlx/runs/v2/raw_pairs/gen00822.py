from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(F.col("device").isin(["mobile", "desktop"]) & F.col("country").isin(["SG", "TH", "DE"]))
    .withColumn("started_at_period", F.date_trunc("week", "started_at"))
    .withColumn("browser_group", F.when(F.col("browser").isin(["Edge", "Safari"]), "Tier 1").otherwise("Other"))
    .orderBy(F.col("customer_id").desc(), "id")
    .limit(3)
)
