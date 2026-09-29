from pyspark.sql import functions as F

adjuster_rows = (
    df_adjusters
    .filter((F.col("level").between(1, 3)) & (F.upper(F.trim(F.col("adjuster_name"))).like("F%")))
    .select("id", F.col("team").alias("name"))
)

adjuster_rows_2 = (
    df_adjusters
    .filter(F.col("team") != "Property")
    .select("id", F.col("team").alias("name"))
)

result = adjuster_rows.intersectAll(adjuster_rows_2)
