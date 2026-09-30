from pyspark.sql import functions as F

result = (
    df_orders
    .filter((F.col("discount") <= 0.35) & F.col("amount").between(500, 1150))
    .select("id", "channel", "store_id")
    .orderBy("id")
    .limit(50)
)
