from pyspark.sql import functions as F

result = (
    df_sessions
    .filter((F.col("browser").isin(["Edge", "Chrome", "Firefox"]) & F.col("customer_id").isin([10, 26, 33])) | ~F.col("device").isin(["desktop", "tablet", "mobile"]))
    .withColumn("browser_group", F.when(F.col("browser").isin(["Firefox", "Edge"]), "Core").otherwise("Other"))
    .withColumn("clean_browser", F.trim("browser"))
    .orderBy(F.col("browser").desc())
)
