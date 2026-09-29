from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("amount").desc(), "id")

base = (
    df_shipments
    .filter((F.col("shipped_date") > F.lit("2023-09-05").cast("date")) & F.col("carrier").isNotNull())
    .select(
        "id",
        "order_id",
        "carrier",
        "shipped_date",
        "delivered_date",
        "cost",
        "status",
        F.round((F.datediff("delivered_date", "shipped_date") / 30), 1).alias("months_shipped_date_to_delivered_date"),
        F.concat_ws("-", "status", "carrier").alias("status_carrier_key"),
        F.coalesce("carrier", F.lit("OTHER")).alias("carrier_clean"),
    )
)

with_dims = (
    base
    .alias("src")
    .join(df_orders.alias("b"), F.col("src.order_id") == F.col("b.id"), "inner")
    .select(
        "src.id",
        "src.order_id",
        "src.carrier",
        "src.shipped_date",
        "src.delivered_date",
        "src.cost",
        "src.carrier_clean",
        F.col("b.discount"),
    )
)

cleaned = (
    with_dims
    .filter((F.col("delivered_date") > F.lit("2023-05-26").cast("date")) & F.col("order_id").isNotNull())
    .withColumn("delivered_date_plus_2d", F.date_add("delivered_date", 2))
    .withColumn("discount_level", F.when(F.col("discount") >= 0.36, "High").when(F.col("discount") >= 0.08, "Medium").otherwise("Low"))
    .withColumn("resolved_carrier", F.coalesce("carrier", F.lit("N/A")))
)

with_dims_shipments = (
    cleaned
    .alias("src")
    .join(df_orders.alias("d"), F.col("src.order_id") == F.col("d.id"), "inner")
    .select("src.*", F.col("d.status"), F.col("d.amount"), F.col("d.channel"))
)

deduped = (
    with_dims_shipments
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
    .drop("dedup_rank")
)

result = (
    deduped
    .groupBy(F.date_trunc("year", "shipped_date").alias("year_start"))
    .agg(
        F.round(F.sum("cost") / F.nullif(F.count(F.when(F.col("status") == "COMPLETED", 1)), F.lit(0)), 2).alias("cost_per_completed"),
        F.count(F.when(F.col("channel") == "mobile", 1)).alias("mobile_rows"),
        F.percentile_approx("discount", 0.5).alias("discount_p50"),
        F.countDistinct("carrier").alias("distinct_carriers"),
    )
    .filter(F.col("discount_p50") >= 0)
    .orderBy(F.col("cost_per_completed").desc())
)
