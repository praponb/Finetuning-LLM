from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("plan").isin(["basic", "standard", "premium"]) | (F.col("plan") == "standard"))
    .orderBy(F.col("id").desc())
    .limit(3)
)
