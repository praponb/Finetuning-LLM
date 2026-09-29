from pyspark.sql import functions as F

holder_side = (
    df_policyholders
    .filter(F.col("segment") == "VIP")
    .select("region")
)

holder_side_2 = (
    df_policyholders
    .filter(F.col("holder_name").isNotNull())
    .select(F.col("holder_name").alias("region"))
)

result = holder_side.intersectAll(holder_side_2)
