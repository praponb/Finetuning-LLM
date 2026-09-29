from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("shipped_date_month_end", "id")
w2 = Window.partitionBy("carrier").orderBy("shipped_date_month_end_2", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("carrier").orderBy("shipped_date_month_end", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("status").orderBy("shipped_date_month_end", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_shipments
    .filter(F.col("delivered_date") >= F.lit("2023-05-19").cast("date"))
    .select(
        "id",
        "order_id",
        "carrier",
        "shipped_date",
        "delivered_date",
        "cost",
        "status",
        F.last_day("shipped_date").alias("shipped_date_month_end"),
        F.datediff("shipped_date", "delivered_date").alias("days_delivered_date_to_shipped_date"),
        F.coalesce("carrier", F.lit("NONE")).alias("carrier_filled"),
    )
)

latest = (
    base
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
)

cleaned = (
    latest
    .filter((F.col("shipped_date") >= F.lit("2024-01-26").cast("date")) & F.col("order_id").isNotNull())
    .select(
        "id",
        "carrier",
        "delivered_date",
        "status",
        "shipped_date_month_end",
        "rn",
        F.last_day("shipped_date").alias("shipped_date_month_end_2"),
        F.add_months("delivered_date", 3).alias("delivered_date_plus_months"),
        F.coalesce("carrier", F.lit("DIRECT")).alias("carrier_clean"),
    )
)

with_windows = (
    cleaned
    .select(
        "*",
        F.last("carrier", ignorenulls=True).over(w2).alias("last_known_carrier"),
        F.last("carrier", ignorenulls=True).over(w3).alias("last_known_carrier_2"),
        F.first("carrier", ignorenulls=True).over(w4).alias("first_known_carrier"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "rn",
        "shipped_date_month_end_2",
        "delivered_date_plus_months",
        "carrier_clean",
        "last_known_carrier",
        "last_known_carrier_2",
        "first_known_carrier",
    )
)
