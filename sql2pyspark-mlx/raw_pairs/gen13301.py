from pyspark.sql import functions as F

result = (
    df_subscriptions
    .select("id", "monthly_fee", F.initcap(F.trim("plan")).alias("plan_title"))
    .orderBy("monthly_fee", "plan_title")
)
