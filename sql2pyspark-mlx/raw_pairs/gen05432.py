from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("monthly_fee") < 240)
    .select("start_date")
    .unionByName(df_inventory.select(F.col("last_restock").alias("start_date")))
    .orderBy(F.col("start_date").desc())
)
