from pyspark.sql import functions as F

listener_rows = (
    df_listeners
    .select(
        F.col("username").alias("label"),
        F.col("country").alias("label_2"),
        F.col("signup_date").alias("day"),
    )
)

listener_rows_2 = df_listeners.select("country", "username", "signup_date")

result = (
    listener_rows
    .union(listener_rows_2)
    .orderBy("label_2")
)
