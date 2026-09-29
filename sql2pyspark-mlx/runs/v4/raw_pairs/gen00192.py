from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("location").isin(["New York", "Chiang Mai", "Tokyo"]))
    .withColumn("adjusted_budget", F.round((F.col("budget") * 1.15), 2))
    .withColumn("dept_name_length", F.length("dept_name"))
    .orderBy("id")
)
