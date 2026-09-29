from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter((F.col("monthly_fee") <= 120) & F.col("plan").isin(["premium", "enterprise"]))
)
