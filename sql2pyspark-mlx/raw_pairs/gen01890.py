from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(F.col("customer_id").isin([1, 9, 21]))
    .select("device", "browser")
    .distinct()
)
