from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("plan").isin(["enterprise", "basic", "standard"]) & (F.col("status") == "CANCELLED"))
    .orderBy(F.col("monthly_fee").desc())
)
