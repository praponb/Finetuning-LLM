from pyspark.sql import functions as F

listener_rows = (
    df_listeners
    .select(F.col("birth_year").alias("ref_id"), F.col("username").alias("label"))
)

listener_rows_2 = (
    df_listeners
    .filter((F.col("country") == "BR") & (F.upper(F.trim(F.col("username"))).like("E%")))
    .select(F.col("birth_year").alias("ref_id"), F.col("username").alias("label"))
)

result = (
    listener_rows
    .intersect(listener_rows_2)
    .orderBy(F.col("ref_id").desc(), "label")
)
