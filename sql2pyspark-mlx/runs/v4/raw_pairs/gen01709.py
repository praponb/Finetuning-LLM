from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("delivered_date", "id")
w2 = Window.partitionBy("status").orderBy(F.col("days_since_shipped_date").desc_nulls_first())
w3 = Window.partitionBy("status")
w4 = Window.orderBy(F.col("days_since_shipped_date").asc_nulls_last())

filtered = (
    df_shipments
    .filter(F.col("order_id").isNotNull())
    .select(
        "id",
        "shipped_date",
        "delivered_date",
        "status",
        F.trim("status").alias("clean_status"),
        F.last_day("delivered_date").alias("delivered_date_month_end"),
    )
)

deduped = (
    filtered
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
)

remaining_keys = (
    deduped
    .select("status", "id")
    .intersectAll(
        df_shipments
        .filter(F.col("delivered_date").between("2024-04-01", "2025-10-19"))
        .select("status", "id")
    )
)

kept_rows = (
    deduped
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.status") == F.col("k.status")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

valid = (
    kept_rows
    .filter((F.col("delivered_date") > F.lit("2023-04-07").cast("date")) & F.month("shipped_date").isin([5, 6, 11]))
    .select(
        "id",
        "shipped_date",
        "delivered_date",
        "status",
        "clean_status",
        "rn",
        F.date_add("shipped_date", 16).alias("shipped_date_plus_16d"),
    )
)

valid_shipments = (
    valid
    .filter(F.col("delivered_date").between("2023-11-01", "2025-10-01"))
    .select(
        "id",
        "shipped_date",
        "delivered_date",
        "status",
        "clean_status",
        "rn",
        "shipped_date_plus_16d",
        F.datediff(F.current_date(), "shipped_date").alias("days_since_shipped_date"),
    )
)

result = (
    valid_shipments
    .select(
        "*",
        F.rank().over(w2).alias("days_since_shipped_date_rank"),
        F.max("days_since_shipped_date").over(w3).alias("status_max_days_since_shipped_date"),
        F.percent_rank().over(w4).alias("days_since_shipped_date_percent_rank"),
        F.sum("days_since_shipped_date").over(w3).alias("status_sum_days_since_shipped_date"),
    )
)
