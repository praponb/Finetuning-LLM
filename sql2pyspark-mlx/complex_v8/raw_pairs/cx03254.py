from pyspark.sql import functions as F

listener_side = (
    df_listeners
    .filter((~F.col("plan").isin("premium", "student")) & (F.lower(F.col("username")).like("%a.%")))
    .select(F.col("birth_year").alias("entity_id"), "signup_date", "plan")
)

listener_rows = df_listeners.select("birth_year", "signup_date", "plan")

result = (
    listener_side
    .union(listener_rows)
    .distinct()
    .orderBy(F.col("signup_date").desc())
)
