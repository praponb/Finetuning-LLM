from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("carrier", "status").orderBy(F.col("cost").asc_nulls_last(), "id")

valid = (
    df_shipments
    .filter(F.col("shipped_date") <= "2024-02-24")
    .select(
        "id",
        "carrier",
        "shipped_date",
        "delivered_date",
        "cost",
        "status",
        F.datediff("delivered_date", "shipped_date").alias("days_shipped_date_to_delivered_date"),
    )
)

latest = (
    valid
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
)

matched_keys = (
    latest
    .select("status", "id")
    .subtract(
        df_shipments
        .filter(F.col("shipped_date") <= "2025-10-01")
        .select("status", "id")
    )
)

kept_rows = (
    latest
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.status") == F.col("k.status")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

remaining_keys = (
    kept_rows
    .select("status")
    .intersectAll(
        df_shipments
        .filter(F.col("cost") <= 32)
        .select("status")
    )
)

reconciled = (
    kept_rows
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.status") == F.col("k.status")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .groupBy(F.date_trunc("year", "delivered_date").alias("year_start"))
    .agg(
        F.size(F.collect_set("carrier")).alias("num_carriers"),
        F.count_if(F.col("carrier") == "Kerry").alias("kerry_events"),
        F.size(F.collect_set("carrier")).alias("num_carriers_2"),
        F.min("cost").alias("min_cost"),
    )
    .filter(F.col("num_carriers") >= 1)
    .orderBy(F.col("year_start").desc())
)
