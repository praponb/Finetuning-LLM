from pyspark.sql import functions as F

base = (
    df_shipments
    .filter((F.col("delivered_date") > F.to_date(F.lit("2023-06-06"))) & F.col("order_id").isNotNull())
    .withColumn("cost_tier", F.when(F.col("cost") >= 96, "High").when(F.col("cost") >= 32, "Medium").otherwise("Low"))
    .withColumn("carrier_filled", F.coalesce("carrier", F.lit("OTHER")))
)

base_shipments = (
    base
    .filter((F.col("delivered_date") > F.lit("2023-06-23").cast("date")) & F.col("carrier").isNotNull())
    .withColumn("days_since_shipped_date", F.datediff(F.current_date(), "shipped_date"))
    .withColumn("resolved_carrier", F.coalesce("carrier", F.lit("OTHER")))
)

cleaned = (
    base_shipments
    .filter(F.col("order_id") == 4)
    .select(
        "*",
        F.trim("carrier").alias("clean_carrier"),
        F.coalesce("carrier", F.lit("OTHER")).alias("carrier_filled_2"),
    )
)

valid = (
    cleaned
    .filter(~F.col("status").isin(["LOST", "RETURNED", "DELIVERED"]))
    .withColumn("weeks_shipped_date_to_delivered_date", (F.datediff("delivered_date", "shipped_date") / 7))
    .withColumn("carrier_upper", F.upper("carrier"))
)

result = (
    valid
    .alias("a")
    .join(df_orders.alias("o"), F.col("a.order_id") == F.col("o.id"), "inner")
    .groupBy(F.col("o.channel").alias("channel"), F.col("a.carrier").alias("carrier"))
    .agg(
        F.count("*").alias("rows_total"),
        F.max(F.col("a.cost")).alias("max_cost"),
        F.avg(F.col("a.days_since_shipped_date")).alias("avg_days_since_shipped_date"),
    )
    .orderBy("carrier")
)
