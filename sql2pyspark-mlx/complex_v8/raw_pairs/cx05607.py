from pyspark.sql import functions as F

adjuster_rows = (
    df_adjusters
    .filter(F.col("level") <= 3)
    .select("adjuster_name", "level", F.col("team").alias("name"))
)

adjuster_side = (
    df_adjusters
    .filter((F.col("level") >= 3) & (F.upper(F.trim(F.col("adjuster_name"))).like("E%")))
    .select("adjuster_name", "id", "team")
)

result = (
    adjuster_rows
    .union(adjuster_side)
    .orderBy("adjuster_name")
)
