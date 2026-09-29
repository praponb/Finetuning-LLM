from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("budget") < 1947000)
    .withColumn("location_group", F.when(F.col("location").isin(["New York", "Bangkok"]), "Core").otherwise("Other"))
    .withColumn("budget_rounded", F.round("budget", 2))
)
