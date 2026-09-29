from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("product_id").isin([4, 5, 10]))
    .select("last_restock")
    .unionByName(
        df_subscriptions
        .filter(F.col("monthly_fee") > 160)
        .select(F.col("start_date").alias("last_restock"))
    )
)
