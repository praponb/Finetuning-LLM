from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("delivered_date", "id")

filtered = (
    df_shipments
    .filter(F.col("order_id").isNotNull() & F.col("shipped_date").between("2024-02-01", "2025-01-29"))
    .withColumn("status_length", F.length("status"))
    .withColumn("carrier_filled", F.coalesce("carrier", F.lit("DIRECT")))
)

cleaned = (
    filtered
    .filter(F.col("carrier").isin(["FedEx", "Flash"]))
    .withColumn("cost_times_status_length", (F.col("cost") * F.col("status_length")))
    .withColumn("resolved_carrier", F.coalesce("carrier", F.lit("N/A")))
)

valid = (
    cleaned
    .filter((F.col("shipped_date") >= F.lit("2023-05-29").cast("date")) & (F.year("shipped_date") == 2023))
    .select(
        "*",
        (F.datediff(F.current_date(), "delivered_date") / 30).alias("months_since_delivered_date"),
        F.coalesce("cost", F.lit(0)).alias("cost_filled"),
    )
)

deduped = (
    valid
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
)

result = (
    deduped
    .withColumn("year_start", F.date_trunc("YEAR", "delivered_date"))
    .withColumn("quarter_start", F.date_trunc("QUARTER", "shipped_date"))
    .groupingSets(
        [["year_start", "carrier", "quarter_start"], ["year_start", "carrier"], []],
        "year_start",
        "carrier",
        "quarter_start",
    )
    .agg(
        F.round(F.avg("status_length"), 2).alias("avg_status_length"),
        F.sum(F.when(F.col("carrier") == "UPS", F.col("cost")).otherwise(0)).alias("ups_cost"),
    )
    .filter(F.col("avg_status_length") >= 0)
    .orderBy(F.col("quarter_start").desc_nulls_first())
)
