from pyspark.sql import functions as F

adjuster_side = (
    df_adjusters
    .filter(F.col("level") >= 1)
    .select(F.col("id").alias("qty"), F.col("adjuster_name").alias("name"), "level")
)

adjuster_side_2 = df_adjusters.select("level", "adjuster_name", "id")

unioned = adjuster_side.union(adjuster_side_2)

result = (
    unioned
    .groupBy("name")
    .agg(F.count("*").alias("n_rows"), F.sum("qty").alias("total_qty"))
    .orderBy("name")
)
