from pyspark.sql import functions as F

base = (
    df_orders
    .filter(F.col("customer_id").isNotNull())
    .withColumn("amount_tier", F.when(F.col("amount") >= 1320, "High").when(F.col("amount") >= 370, "Medium").otherwise("Low"))
    .withColumn("discount_whole", F.ceil("discount"))
)

result = (
    base
    .select("id")
    .intersectAll(
        df_orders
        .filter(F.col("customer_id").isin([5, 25, 35]))
        .select("id")
    )
)
