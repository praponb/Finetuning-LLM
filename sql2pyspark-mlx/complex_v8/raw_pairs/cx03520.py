from pyspark.sql import functions as F

adjuster_side = df_adjusters.select(F.col("id").alias("key_id"))

adjuster_side_2 = (
    df_adjusters
    .filter(F.col("team") == "Travel")
    .select(F.col("id").alias("key_id"))
)

result = adjuster_side.subtract(adjuster_side_2)
