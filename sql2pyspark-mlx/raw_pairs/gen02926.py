from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status", "store_id").orderBy(F.col("order_date").desc(), "id")
w2 = Window.partitionBy("channel").orderBy(F.col("sqft").desc())

cleaned = (
    df_orders
    .filter(F.year("order_date") >= 2024)
    .select("*", F.concat("status", F.lit(", "), "channel").alias("status_channel"))
)

deduped = (
    cleaned
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
    .drop("latest_rank")
)

joined = (
    deduped
    .alias("src")
    .join(df_stores.alias("b"), F.col("src.store_id") == F.col("b.id"), "inner")
    .select(
        "src.id",
        "src.customer_id",
        "src.store_id",
        "src.channel",
        "src.status_channel",
        F.col("b.region"),
        F.col("b.store_name"),
        F.col("b.sqft"),
    )
)

deduped_orders = (
    joined
    .withColumn("latest_rank", F.rank().over(w2))
    .filter(F.col("latest_rank") == 1)
    .select("id", "customer_id", "status_channel", "region", "sqft")
)

joined_orders = (
    deduped_orders
    .alias("e")
    .join(df_customers.alias("b"), F.col("e.customer_id") == F.col("b.id"), "inner")
    .filter(F.col("b.country") != "UK")
    .select(
        "e.id",
        "e.customer_id",
        "e.status_channel",
        "e.region",
        F.col("b.signup_date"),
        F.col("b.city"),
    )
)

result = (
    joined_orders
    .groupBy("region", "city")
    .agg(F.count("*").alias("cnt"), F.count("*").alias("cnt_2"))
)
