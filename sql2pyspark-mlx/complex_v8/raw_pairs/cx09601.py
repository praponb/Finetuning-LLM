from pyspark.sql import functions as F

holder_side = df_policyholders.select("id", "joined_on")

holder_side_2 = df_policyholders.select("id", "joined_on")

result = (
    holder_side
    .union(holder_side_2)
    .distinct()
    .orderBy("id", "joined_on")
)
