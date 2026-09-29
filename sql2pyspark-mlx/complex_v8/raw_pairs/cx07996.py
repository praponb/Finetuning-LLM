from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_max_voltage_bucket_2 = Window.orderBy("max_voltage_bucket_2", "read_at_ym", "site_id").rowsBetween(-6, Window.currentRow)
w_all_by_max_voltage_bucket_2_2 = Window.orderBy("max_voltage_bucket_2", "read_at_ym", "site_id")

valid_readings = (
    df_meter_readings
    .filter((F.col("site_id").isNotNull()) | (F.col("quality").isin("BAD", "EST")))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        (F.floor(F.col("voltage") / 5) * 5).alias("voltage_bucket"),
        (F.floor(F.col("voltage") / 5) * 5).alias("voltage_bucket_2"),
        (F.col("read_at") + F.expr("INTERVAL 30 MINUTES")).alias("read_at_plus_30m"),
    )
)

quality_stats = (
    valid_readings
    .groupBy("quality")
    .agg(F.percentile_approx("voltage_bucket", 0.5).alias("median_voltage_bucket"))
)

above_average = (
    valid_readings.alias("s")
    .join(quality_stats.alias("g"), F.col("g.quality") == F.col("s.quality"), "inner")
    .filter(F.col("s.voltage_bucket") >= F.col("g.median_voltage_bucket"))
    .select(
        "s.*",
        "g.median_voltage_bucket",
        F.round(F.col("s.voltage_bucket") - F.col("g.median_voltage_bucket"), 2).alias("voltage_bucket_minus_median"),
    )
)

rollup_base = (
    above_average
    .groupBy(F.date_format("read_at", "yyyy-MM").alias("read_at_ym"), "site_id")
    .agg(
        F.max("voltage_bucket_2").alias("max_voltage_bucket_2"),
        F.array_sort(F.collect_set("quality")).alias("quality_list"),
        F.count("*").alias("n_rows"),
        F.min("kwh").alias("min_kwh"),
    )
)

enriched = (
    rollup_base
    .filter(F.col("n_rows").between(4, 9))
    .select(
        "read_at_ym",
        "site_id",
        "max_voltage_bucket_2",
        "quality_list",
        "n_rows",
        F.lower(F.regexp_replace(F.trim(F.col("read_at_ym")), " ", "_")).alias("read_at_ym_slug"),
    )
)

sequenced = (
    enriched
    .withColumn("n_rows_sum_7", F.sum("n_rows").over(w_all_by_max_voltage_bucket_2))
    .withColumn("seq_no", F.row_number().over(w_all_by_max_voltage_bucket_2_2))
)

result = (
    sequenced
    .select(
        "read_at_ym",
        "site_id",
        "n_rows_sum_7",
        "max_voltage_bucket_2",
        "n_rows",
        "seq_no",
    )
    .unpivot(
        ids=["read_at_ym", "site_id"],
        values=["n_rows_sum_7", "max_voltage_bucket_2", "n_rows", "seq_no"],
        variableColumnName="field",
        valueColumnName="amount",
    )
    .filter(F.col("amount").isNotNull())
)
