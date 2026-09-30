from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel").orderBy(F.col("discount").desc_nulls_first(), "id")
w2 = Window.partitionBy("status").orderBy(F.col("channel_length").desc())
revenue_by_store_id = df_store_sales.groupBy("store_id").agg(F.max("revenue").alias("max_store_sale_revenue"))

valid = (
    df_orders
    .filter(F.col("order_date") >= F.lit("2023-09-28").cast("date"))
    .select("*", F.length("channel").alias("channel_length"))
)

latest = (
    valid
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
    .select("id", "store_id", "status", "channel", "channel_length")
)

joined = (
    latest
    .alias("src")
    .join(df_stores.alias("s"), F.col("src.store_id") == F.col("s.id"), "inner")
    .select("src.*", F.col("s.city"), F.col("s.sqft"))
)

ranked = (
    joined
    .withColumn("dedup_rank", F.rank().over(w2))
    .filter(F.col("dedup_rank") == 1)
)

result = (
    ranked
    .join(revenue_by_store_id, on="store_id", how="left")
    .select(
        "id",
        "store_id",
        "channel",
        "channel_length",
        "city",
        "dedup_rank",
        F.when(F.col("city").rlike("^(Bangkok|Tokyo)$"), "CORE").when(F.col("city").rlike("^(Chiang Mai|New York)$"), "EXTENDED").otherwise("GROUP_B").alias("city_zone"),
        F.col("max_store_sale_revenue"),
    )
    .orderBy("channel", "channel_length")
)
