from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("monthly_fee") > 230)
    .select("start_date", F.ceil("monthly_fee").alias("monthly_fee_whole"))
    .orderBy(F.col("start_date").desc(), F.col("monthly_fee_whole").desc())
)
