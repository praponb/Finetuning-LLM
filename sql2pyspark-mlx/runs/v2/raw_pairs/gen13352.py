from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter((F.col("end_date") <= "2024-03-01") & F.col("plan").isin(["premium", "standard", "enterprise"]))
    .select("id", "monthly_fee")
    .orderBy(F.col("monthly_fee").desc())
)
