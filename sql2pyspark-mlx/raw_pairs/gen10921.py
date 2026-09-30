from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(F.col("browser").isNull() & (F.col("customer_id") == 40))
    .withColumn("browser_upper", F.upper("browser"))
    .withColumn("browser_lower", F.lower("browser"))
)
