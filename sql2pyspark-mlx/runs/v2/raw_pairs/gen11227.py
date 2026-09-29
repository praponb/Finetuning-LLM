from pyspark.sql import functions as F

result = (
    df_customers
    .withColumn("segment_group", F.when(F.col("segment").isin(["SMB", "Corporate"]), "Core").otherwise("Other"))
)
