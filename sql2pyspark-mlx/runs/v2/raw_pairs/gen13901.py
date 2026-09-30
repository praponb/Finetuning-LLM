from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(F.col("customer_id").isNull())
    .select("country")
    .distinct()
    .orderBy("country")
    .limit(5)
)
