from pyspark.sql import functions as F

adjuster_side = df_adjusters.select(F.col("id").alias("qty"), "level")

adjuster_side_2 = (
    df_adjusters
    .filter((F.col("level") < 3) & (F.col("adjuster_name").isNotNull()))
    .select("id", "level")
)

result = adjuster_side.union(adjuster_side_2)
