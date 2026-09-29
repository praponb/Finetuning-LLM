from pyspark.sql import functions as F

adjuster_side = df_adjusters.select("level")

adjuster_side_2 = df_adjusters.select("level")

result = (
    adjuster_side
    .subtract(adjuster_side_2)
    .orderBy("level")
)
