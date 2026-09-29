from pyspark.sql import functions as F

result = (
    df_sessions
    .filter((F.col("browser") == "Safari") | F.col("customer_id").isNotNull())
)
