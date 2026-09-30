from pyspark.sql import functions as F

adjuster_rows = (
    df_adjusters
    .filter((F.col("level") > 2) & (F.upper(F.trim(F.col("adjuster_name"))).like("A%")))
    .select(F.col("adjuster_name").alias("label"))
)

adjuster_rows_2 = (
    df_adjusters
    .filter((F.lower(F.col("adjuster_name")).like("%rt%")) & (F.col("level") < 3))
    .select(F.col("adjuster_name").alias("label"))
)

result = (
    adjuster_rows
    .intersect(adjuster_rows_2)
    .orderBy(F.col("label").desc())
)
