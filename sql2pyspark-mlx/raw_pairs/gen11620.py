from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(F.col("customer_id").isNull() & ~F.col("country").isin(["JP", "DE"]))
)
