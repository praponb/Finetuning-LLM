from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy(F.col("shipped_date").desc_nulls_last(), "id")
w2 = Window.partitionBy("carrier").orderBy(F.col("delivered_date").desc())
w3 = Window.orderBy(F.col("delivered_date").desc(), "id")
w4 = Window.partitionBy("carrier").orderBy("delivered_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("carrier").orderBy("rn", "id")

cleaned = (
    df_shipments
    .filter((F.col("delivered_date") >= F.lit("2023-07-24").cast("date")) & F.col("order_id").isNotNull())
    .select(
        "id",
        "order_id",
        "carrier",
        "shipped_date",
        "delivered_date",
        "cost",
        "status",
        (F.datediff("shipped_date", "delivered_date") / 7).alias("weeks_delivered_date_to_shipped_date"),
        F.coalesce("carrier", F.lit("N/A")).alias("carrier_filled"),
    )
)

ranked = (
    cleaned
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
    .select("id", "carrier", "delivered_date")
)

filtered = (
    ranked
    .filter(F.col("carrier").isNotNull() & (F.col("delivered_date") < "2025-08-02"))
    .select(
        "*",
        F.lower("carrier").alias("carrier_lower"),
        F.trim("carrier").alias("clean_carrier"),
    )
)

ranked_shipments = (
    filtered
    .withColumn("rn", F.rank().over(w2))
    .filter(F.col("rn") == 1)
)

with_windows = (
    ranked_shipments
    .filter(F.col("delivered_date").between("2023-10-08", "2024-08-01"))
    .select(
        "*",
        F.row_number().over(w3).alias("seq_no"),
        F.last("carrier", ignorenulls=True).over(w4).alias("last_known_carrier"),
        F.row_number().over(w5).alias("seq_no_2"),
    )
)

report = (
    with_windows
    .select(
        "id",
        "carrier",
        "carrier_lower",
        "clean_carrier",
        "rn",
        "seq_no",
        "last_known_carrier",
        "seq_no_2",
    )
)

result = (
    report
    .select(
        "id",
        "carrier_lower",
        "clean_carrier",
        "rn",
        "seq_no",
        "last_known_carrier",
        "seq_no_2",
    )
    .orderBy(F.col("seq_no").desc_nulls_last(), "id")
)
