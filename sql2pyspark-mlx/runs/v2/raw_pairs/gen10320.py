from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(F.col("customer_id") == 27)
    .select("device", "browser")
    .distinct()
    .orderBy(F.col("browser").desc())
)
