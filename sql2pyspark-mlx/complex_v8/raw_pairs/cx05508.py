from pyspark.sql import functions as F

listener_side = (
    df_listeners
    .filter(F.size(F.col("interests")) >= 2)
    .select(F.col("country").alias("name"), F.col("plan").alias("category"), "id")
)

listener_side_2 = (
    df_listeners
    .select(F.col("country").alias("name"), F.col("username").alias("category"), "id")
)

result = listener_side.unionByName(listener_side_2)
