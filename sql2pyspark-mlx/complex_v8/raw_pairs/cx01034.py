from pyspark.sql import functions as F

listener_rows = (
    df_listeners
    .select(F.col("birth_year").alias("key_id"), F.col("signup_date").alias("day"))
)

listener_side = (
    df_listeners
    .filter((F.col("birth_year").between(1967, 1988)) & (F.col("country").isNotNull()))
    .select(F.col("birth_year").alias("key_id"), F.col("signup_date").alias("day"))
)

combined = listener_rows.unionByName(listener_side)

result = (
    combined
    .groupBy("key_id")
    .agg(F.count("*").alias("n_rows"), F.max("day").alias("latest_day"))
    .orderBy("key_id")
)
