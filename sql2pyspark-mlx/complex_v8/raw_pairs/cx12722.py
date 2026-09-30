from pyspark.sql import functions as F

listener_side = (
    df_listeners
    .filter(~F.col("country").isin("KR", "JP"))
    .select(F.col("birth_year").alias("ref_id"))
)

listener_rows = (
    df_listeners
    .filter((F.col("plan").isin("free", "student")) & (F.year(F.col("signup_date")) == 2024))
    .select(F.col("birth_year").alias("ref_id"))
)

result = (
    listener_side
    .subtract(listener_rows)
    .orderBy("ref_id")
)
