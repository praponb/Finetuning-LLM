from pyspark.sql import functions as F

scoped_listeners = (
    df_listeners
    .filter((F.col("country").isin("KR", "TH")) & (F.length(F.trim(F.col("username"))) >= 5))
)

listener_side = (
    scoped_listeners
    .filter((F.col("signup_date") >= F.lit("2024-10-17").cast("date")) & (F.col("country") == "UK"))
    .select("birth_year", F.col("id").alias("ref_id"))
)

listener_rows = (
    df_listeners
    .filter(F.col("plan").isin("student", "family", "free"))
    .select(F.col("id").alias("birth_year"), F.col("birth_year").alias("ref_id"))
)

listener_rows_2 = df_listeners.select("birth_year", F.col("id").alias("ref_id"))

result = (
    listener_side
    .unionByName(listener_rows)
    .unionByName(listener_rows_2)
    .orderBy("birth_year", "ref_id")
)
