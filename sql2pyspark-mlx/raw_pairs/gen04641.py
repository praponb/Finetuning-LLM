from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("carrier", "order_id")

base = (
    df_shipments
    .filter(F.col("order_id").isNotNull())
    .withColumn("status_title", F.initcap(F.trim("status")))
    .withColumn("status_prefix", F.substring("status", 1, 4))
)

remaining_keys = (
    base
    .select("order_id")
    .subtract(
        df_shipments
        .filter(F.col("cost") > 89)
        .select("order_id")
    )
)

verified = (
    base
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.order_id") == F.col("k.order_id")), "inner")
    .select("s.*")
)

enriched = (
    verified
    .select(
        "carrier",
        "shipped_date",
        "delivered_date",
        "cost",
        "status_title",
        "status_prefix",
        F.sum("cost").over(w).alias("carrier_sum_cost"),
    )
)

cleaned = (
    enriched
    .filter((F.col("delivered_date") >= F.lit("2023-10-01").cast("date")) & (F.col("carrier_sum_cost") <= 478))
    .select(
        "carrier",
        "shipped_date",
        "delivered_date",
        "cost",
        "status_title",
        "carrier_sum_cost",
        F.length("carrier").alias("carrier_length"),
        F.coalesce("carrier", F.lit("N/A")).alias("carrier_filled"),
    )
)

base_shipments = (
    cleaned
    .filter(F.col("carrier").isNotNull() & (F.col("carrier_sum_cost") >= 170))
    .select(
        "carrier",
        "delivered_date",
        "carrier_sum_cost",
        "carrier_length",
        "carrier_filled",
        F.datediff(F.current_date(), "delivered_date").alias("days_since_delivered_date"),
        F.coalesce("carrier_length", F.lit(0)).alias("carrier_length_filled"),
    )
)

result = (
    base_shipments
    .groupBy(
        F.when(F.col("carrier_sum_cost") >= 450, "HIGH").otherwise("LOW").alias("carrier_sum_cost_tier"),
        F.when(F.col("carrier_length") >= 77, "PREMIUM").otherwise("STANDARD").alias("carrier_length_tier"),
    )
    .agg(
        F.count_if(F.col("carrier") == "Flash").alias("flash_events"),
        F.size(F.collect_set("carrier")).alias("num_carriers"),
        F.count(F.when(F.col("carrier") == "Flash", 1)).alias("flash_count"),
        F.max(F.when(F.col("carrier") == "DHL", F.col("carrier_length"))).alias("dhl_max_carrier_length"),
    )
    .filter(F.col("flash_count") > 1)
    .orderBy(F.col("carrier_sum_cost_tier").asc_nulls_last())
)
