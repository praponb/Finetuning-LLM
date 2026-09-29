from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("carrier").orderBy("cost_rounded")
w2 = Window.orderBy(F.col("cost_rounded").asc_nulls_last())

valid = (
    df_shipments
    .filter((F.col("delivered_date") >= F.to_date(F.lit("2023-06-01"))) & (F.col("shipped_date") <= "2024-12-30"))
    .select(
        "id",
        "carrier",
        "shipped_date",
        "delivered_date",
        "status",
        F.round("cost", 2).alias("cost_rounded"),
        F.concat("carrier", F.lit(" "), "status").alias("carrier_status"),
    )
)

matched_keys = (
    valid
    .select("carrier")
    .exceptAll(
        df_shipments
        .filter(F.col("order_id").isNull())
        .select("carrier")
    )
)

verified = (
    valid
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.carrier") == F.col("k.carrier")), "inner")
    .select("s.*")
)

filtered = (
    verified
    .filter((F.col("delivered_date") >= F.lit("2023-06-03").cast("date")) & (F.col("shipped_date") >= "2023-07-19"))
    .withColumn("carrier_upper", F.upper("carrier"))
    .withColumn("cost_rounded_level", F.when(F.col("cost_rounded") >= 89, "High").when(F.col("cost_rounded") >= 15, "Medium").otherwise("Low"))
    .withColumn("carrier_filled", F.coalesce("carrier", F.lit("N/A")))
)

remaining_keys = (
    filtered
    .select("status", "carrier")
    .subtract(
        df_shipments
        .filter(F.col("cost").between(56, 65))
        .select("status", "carrier")
    )
)

reconciled = (
    filtered
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.status") == F.col("k.status")) & (F.col("s.carrier") == F.col("k.carrier")), "inner")
    .select("s.*")
)

with_windows = reconciled.select("*", F.cume_dist().over(w).alias("cost_rounded_cume_dist"))

scored = (
    with_windows
    .filter(F.col("shipped_date") <= "2025-07-01")
    .select(
        "delivered_date",
        "cost_rounded",
        "carrier_status",
        F.rank().over(w2).alias("cost_rounded_rank"),
    )
)

base = (
    scored
    .filter((F.col("delivered_date") > F.lit("2023-08-16").cast("date")) & (F.col("cost_rounded_rank") > 3))
    .withColumn("cost_rounded_level", F.when(F.col("cost_rounded") >= 66, "High").when(F.col("cost_rounded") >= 10, "Medium").otherwise("Low"))
    .withColumn("years_since_delivered_date", (F.datediff(F.current_date(), "delivered_date") / 365))
)

result = (
    base
    .select("cost_rounded", "carrier_status", "cost_rounded_rank")
    .orderBy(F.col("cost_rounded_rank").desc())
)
