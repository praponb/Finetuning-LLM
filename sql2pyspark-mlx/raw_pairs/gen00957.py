from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.partitionBy("carrier").orderBy("delivered_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy(F.month("shipped_date_period")).orderBy(F.col("cost").desc(), "id")

filtered = (
    df_shipments
    .filter((F.col("delivered_date") > F.lit("2023-09-12").cast("date")) & F.col("order_id").isNotNull())
    .select(
        "id",
        "order_id",
        "carrier",
        "shipped_date",
        "delivered_date",
        "cost",
        "status",
        F.date_trunc("MONTH", "shipped_date").alias("shipped_date_period"),
    )
)

cleaned = (
    filtered
    .filter(F.col("order_id").isNotNull() & F.col("order_id").isNotNull())
    .select(
        "id",
        "order_id",
        "carrier",
        "shipped_date",
        "delivered_date",
        "cost",
        "shipped_date_period",
        F.when(F.col("cost") >= 73, "High").when(F.col("cost") >= 27, "Medium").otherwise("Low").alias("cost_tier"),
        F.lower("status").alias("status_lower"),
        F.coalesce("carrier", F.lit("OTHER")).alias("carrier_filled"),
    )
)

with_windows = (
    cleaned
    .filter(F.col("shipped_date_period") >= "2025-03-04")
    .select(
        "id",
        "order_id",
        "carrier",
        "shipped_date",
        F.sum("cost").over(w_hist).alias("running_cost"),
        F.ntile(3).over(w2).alias("cost_bucket"),
    )
)

cleaned_shipments = (
    with_windows
    .filter((F.col("shipped_date") > F.lit("2023-09-22").cast("date")) & F.col("order_id").isNotNull())
    .withColumn("carrier_lower", F.lower("carrier"))
    .withColumn("carrier_filled", F.coalesce("carrier", F.lit("N/A")))
)

joined = (
    cleaned_shipments
    .alias("s")
    .join(df_orders.alias("p"), F.col("s.order_id") == F.col("p.id"), "left")
    .select(
        "s.id",
        "s.order_id",
        "s.carrier",
        "s.shipped_date",
        "s.cost_bucket",
        "s.carrier_lower",
        F.col("p.discount"),
        F.col("p.order_date"),
    )
)

result = (
    joined
    .groupBy("order_id")
    .agg(
        F.count_if(F.col("carrier") == "DHL").alias("dhl_events"),
        F.count_if(F.col("carrier") == "FedEx").alias("fedex_events"),
        F.countDistinct("order_id").alias("distinct_order_ids"),
        F.round(F.sum("discount") / F.nullif(F.count(F.when(F.col("carrier") == "UPS", 1)), F.lit(0)), 2).alias("discount_per_ups"),
    )
    .orderBy("dhl_events")
)
