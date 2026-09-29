from pyspark.sql import functions as F

filtered = (
    df_meter_readings
    .filter((F.col("quality") == "BAD") | (F.col("read_at") >= F.lit("2025-02-06 12:00:00").cast("timestamp")))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
    )
)

stats = (
    filtered
    .groupBy("site_id", F.year("read_at").alias("read_at_year"))
    .agg(
        F.count("*").alias("n_rows"),
        F.max("read_at").alias("last_read_at"),
        F.min("read_at").alias("first_read_at"),
        F.round(F.avg("voltage"), 2).alias("avg_voltage"),
        F.sum("voltage").alias("sum_voltage_check"),
    )
    .filter(F.col("sum_voltage_check") > 222.5)
    .drop("sum_voltage_check")
)

annotated = (
    stats
    .filter(F.month(F.col("last_read_at")).isin(1, 3))
    .select(
        "*",
        F.round(F.col("n_rows") / F.nullif(F.col("read_at_year"), F.lit(0)), 3).alias("n_rows_per_read_at_year"),
        (F.col("last_read_at") + F.expr("INTERVAL 7 DAYS")).alias("last_read_at_plus_7d"),
        F.date_trunc("week", F.col("last_read_at")).alias("last_read_at_week"),
    )
)

result = (
    annotated
    .filter(F.col("site_id").isNotNull())
    .select(
        "site_id",
        "read_at_year",
        "n_rows",
        "last_read_at",
        "avg_voltage",
        "last_read_at_plus_7d",
        "last_read_at_week",
        F.month(F.col("first_read_at")).alias("first_read_at_month"),
    )
    .orderBy(F.col("read_at_year").desc_nulls_first())
)
