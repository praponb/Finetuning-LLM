from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "left")
    .groupBy(
        F.when(F.col("s.city").isin(["Tokyo", "New York"]), "tier_1").otherwise("other").alias("city_group"),
    )
    .agg(
        F.max(F.col("o.amount")).alias("max_amount"),
        F.count(F.col("o.amount")).alias("amount_count"),
    )
    .orderBy("city_group", F.col("amount_count").desc())
    .limit(20)
)
