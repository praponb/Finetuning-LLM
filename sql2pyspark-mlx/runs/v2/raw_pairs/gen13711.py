from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("age") < 41)
    .withColumn("is_active_flag", F.col("is_active").cast("int"))
    .withColumn("segment_group", F.when(F.col("segment").isin(["Consumer", "SMB"]), "Priority").otherwise("Other"))
    .orderBy(F.col("city").desc(), "segment_group")
)
