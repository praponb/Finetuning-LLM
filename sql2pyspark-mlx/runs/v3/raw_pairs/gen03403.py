from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(F.hour("started_at").between(11, 14) & F.col("customer_id").isin([3, 15]))
    .withColumn("started_at_date", F.to_date("started_at"))
)
