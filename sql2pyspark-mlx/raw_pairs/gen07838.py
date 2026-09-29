from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("level") != "L1")
    .withColumn("level_prefix", F.substring("level", 1, 4))
    .withColumn("level_group", F.when(F.col("level").isin(["L2", "L3"]), "Priority").otherwise("Other"))
)
