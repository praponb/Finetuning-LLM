from pyspark.sql import functions as F

listener_side = df_listeners.select(F.col("birth_year").alias("ref_id"), "plan")

listener_side_2 = df_listeners.select("id", "country")

stacked = listener_side.union(listener_side_2)

result = (
    stacked
    .groupBy("plan")
    .agg(F.count("*").alias("n_rows"), F.sum("ref_id").alias("total_ref_id"))
    .orderBy("plan")
)
