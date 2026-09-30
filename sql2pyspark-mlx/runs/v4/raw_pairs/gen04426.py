from pyspark.sql import functions as F

valid = (
    df_shipments
    .filter(F.col("shipped_date") >= F.lit("2023-03-28").cast("date"))
    .select(
        "id",
        "order_id",
        "carrier",
        "delivered_date",
        "cost",
        F.coalesce("carrier", F.lit("unassigned")).alias("carrier_filled"),
        F.date_trunc("YEAR", "delivered_date").alias("delivered_date_period"),
    )
)

base = (
    valid
    .filter((F.col("delivered_date") >= F.lit("2023-04-09").cast("date")) & F.col("order_id").isNotNull())
    .select(
        "id",
        "order_id",
        "carrier",
        "carrier_filled",
        F.initcap(F.trim("carrier")).alias("carrier_title"),
    )
)

filtered = (
    base
    .filter(F.col("order_id") == 19)
    .withColumn("carrier_group", F.when(F.col("carrier").isin(["Kerry", "UPS"]), "Tier 1").otherwise("Other"))
    .withColumn("carrier_title_2", F.initcap(F.trim("carrier")))
    .withColumn("carrier_clean", F.coalesce("carrier", F.lit("UNKNOWN")))
)

result = (
    filtered
    .groupBy("carrier")
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.count(F.when(F.col("carrier") == "Kerry", 1)).alias("kerry_rows"),
        F.count(F.when(F.col("carrier") == "FedEx", 1)).alias("fedex_count"),
        F.count(F.when(F.col("carrier") == "FedEx", 1)).alias("fedex_count_2"),
    )
    .orderBy(F.col("fedex_count_2").desc_nulls_last())
)
