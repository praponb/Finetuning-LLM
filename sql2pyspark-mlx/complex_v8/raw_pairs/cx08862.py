from pyspark.sql import functions as F

adjuster_rows = df_adjusters.select("level")

holder_rows = (
    df_policyholders
    .filter((F.upper(F.trim(F.col("holder_name"))).like("N%")) & (F.col("region") == "Central"))
    .select(F.col("id").alias("level"))
)

result = (
    adjuster_rows
    .subtract(holder_rows)
    .orderBy(F.col("level").desc())
)
