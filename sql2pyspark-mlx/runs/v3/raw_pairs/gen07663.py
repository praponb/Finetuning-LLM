from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("location").isin(["Berlin", "Chiang Mai"]))
    .withColumn("budget_level", F.when(F.col("budget") >= 3832000, "High").when(F.col("budget") >= 1885000, "Medium").otherwise("Low"))
    .orderBy("location", F.col("id").desc())
)
