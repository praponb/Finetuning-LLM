from pyspark.sql import functions as F

filtered_departments = (
    df_departments
    .filter((F.col("location") != "Chiang Mai") & (F.col("budget") > 1371000))
)

summary = (
    filtered_departments
    .groupBy("location")
    .agg(F.count("budget").alias("budget_count"))
)

result = (
    summary
    .select("location", "budget_count")
    .orderBy("budget_count")
)
