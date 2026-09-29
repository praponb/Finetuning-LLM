from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("plan") == "standard")
    .select("id", "end_date")
    .orderBy(F.col("id").desc())
    .limit(10)
)
