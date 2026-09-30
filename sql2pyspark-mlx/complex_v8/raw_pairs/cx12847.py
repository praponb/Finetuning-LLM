from pyspark.sql import functions as F

listener_rows = df_listeners.select("id")

listener_rows_2 = df_listeners.select(F.col("birth_year").alias("id"))

result = (
    listener_rows
    .exceptAll(listener_rows_2)
    .orderBy("id")
)
