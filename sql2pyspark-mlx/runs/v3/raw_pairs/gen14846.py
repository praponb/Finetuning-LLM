from pyspark.sql import functions as F

result = (
    df_inventory
    .select("last_restock")
    .unionByName(
        df_subscriptions
        .filter(F.col("start_date") > "2024-09-01")
        .select(F.col("end_date").alias("last_restock"))
    )
    .orderBy("last_restock")
)
