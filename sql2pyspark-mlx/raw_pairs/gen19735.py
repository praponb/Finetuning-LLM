from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status", "carrier").orderBy(F.col("delivered_date").desc_nulls_last())
w2 = Window.partitionBy("order_id").orderBy("row_num_adjusted")
w3 = Window.orderBy(F.col("row_num_adjusted").desc(), "id")

filtered = (
    df_shipments
    .filter((F.col("delivered_date") >= F.to_date(F.lit("2023-04-13"))) & F.col("carrier").isNotNull())
    .select(
        "*",
        F.concat_ws("-", "carrier", "status").alias("carrier_status_key"),
        F.lower("carrier").alias("carrier_lower"),
        F.coalesce("carrier", F.lit("NONE")).alias("carrier_filled"),
    )
)

latest = (
    filtered
    .withColumn("row_num", F.rank().over(w))
    .filter(F.col("row_num") == 1)
)

filtered_shipments = (
    latest
    .filter((F.col("delivered_date") >= F.lit("2023-09-17").cast("date")) & F.col("order_id").isNotNull())
    .withColumn("row_num_adjusted", F.round((F.col("row_num") * 0.85), 2))
)

enriched = (
    filtered_shipments
    .filter(F.col("cost") < 96)
    .select(
        "id",
        "order_id",
        "carrier",
        "cost",
        "carrier_status_key",
        "carrier_lower",
        "carrier_filled",
        F.percent_rank().over(w2).alias("row_num_adjusted_percent_rank"),
        F.ntile(5).over(w3).alias("row_num_adjusted_bucket"),
    )
)

valid = (
    enriched
    .filter(F.col("carrier").isNotNull())
    .select(
        "*",
        F.lower("carrier").alias("carrier_lower_2"),
        F.col("cost").cast("int").alias("cost_int"),
    )
)

wide = (
    valid
    .groupBy("order_id")
    .pivot("carrier", ["UPS", "Flash"])
    .agg(
        F.sum("row_num_adjusted_bucket").alias("amount"),
        F.count(F.lit(1)).alias("item_count"),
    )
    .select(
        "order_id",
        F.col("UPS_amount").alias("ups_amount"),
        F.col("UPS_item_count").alias("ups_item_count"),
        F.col("Flash_amount").alias("flash_amount"),
        F.col("Flash_item_count").alias("flash_item_count"),
    )
)

result = (
    wide
    .select(
        "order_id",
        "ups_amount",
        "ups_item_count",
        "flash_amount",
        "flash_item_count",
    )
)
