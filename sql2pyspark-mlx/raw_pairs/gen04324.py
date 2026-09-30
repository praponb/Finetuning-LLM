from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id")
w_part = Window.orderBy(F.col("created_at").cast("bigint")).rangeBetween(-7200, Window.currentRow)
w3 = Window.partitionBy("attr_key", "customer_id").orderBy(F.col("created_at_hour").asc_nulls_last())

exploded = (
    df_carts
    .select(
        "id",
        "customer_id",
        "created_at",
        F.explode_outer("attributes").alias("attr_key", "value"),
    )
)

cleaned = (
    exploded
    .filter(F.col("attr_key").isNotNull())
    .withColumn("created_at_hour", F.hour("created_at"))
    .withColumn("attr_key_title", F.initcap(F.trim("attr_key")))
)

with_windows = (
    cleaned
    .withColumn("customer_id_max_created_at_hour", F.max("created_at_hour").over(w))
    .withColumn("created_at_hour_max_2h", F.max("created_at_hour").over(w_part))
    .withColumn("created_at_hour_dense_rank", F.dense_rank().over(w3))
)

kept_keys = (
    with_windows
    .select("customer_id", "id")
    .subtract(
        df_carts
        .filter(F.to_date("created_at") >= "2024-09-15")
        .select("customer_id", "id")
    )
)

verified = (
    with_windows
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.customer_id") == F.col("k.customer_id")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    verified
    .groupBy(F.date_trunc("DAY", "created_at").alias("activity_day"))
    .agg(
        F.count("*").alias("event_count"),
        F.min("customer_id_max_created_at_hour").alias("min_customer_id_max_created_at_hour"),
        F.count(F.when(F.col("attr_key") == "device", 1)).alias("device_rows"),
    )
)
