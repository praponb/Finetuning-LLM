from pyspark.sql import functions as F

adjuster_rows = (
    df_adjusters
    .filter(F.col("adjuster_name").isNotNull())
    .select("id")
)

adjuster_side = (
    df_adjusters
    .filter(F.col("team").isNotNull())
    .select("id")
)

result = adjuster_rows.subtract(adjuster_side)
