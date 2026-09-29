from pyspark.sql import functions as F

listener_rows = df_listeners.select(F.col("id").alias("ref_id"), F.col("username").alias("label"))

listener_side = (
    df_listeners
    .filter(F.col("signup_date") >= F.lit("2024-08-02").cast("date"))
    .select(F.col("id").alias("ref_id"), F.col("country").alias("label"))
)

result = listener_rows.subtract(listener_side)
