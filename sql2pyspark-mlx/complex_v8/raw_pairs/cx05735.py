from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("site_id").orderBy(F.col("read_at").desc(), F.col("id").desc())

cleaned_readings = (
    df_meter_readings
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        F.round(F.col("voltage") * 1.1, 2).alias("voltage_adj"),
        F.length(F.trim(F.col("quality"))).alias("quality_len"),
        F.substring(F.trim(F.col("quality")), 1, 4).alias("quality_prefix"),
    )
)

combined_details = (
    cleaned_readings.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "inner")
    .select(
        "s.id",
        "s.site_id",
        "s.read_at",
        "s.voltage",
        "s.voltage_adj",
        "si.site_name",
    )
)

annotated = (
    combined_details
    .select(
        "*",
        F.round(F.col("voltage") * 0.25, 2).alias("voltage_adj_2"),
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("read_at")).alias("days_since_read_at"),
        F.date_format(F.col("read_at"), "yyyy-MM").alias("read_at_label"),
    )
)

latest_per_key = (
    annotated
    .withColumn("recency_rank", F.row_number().over(w_dedup))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

scored = (
    latest_per_key
    .select(
        "*",
        F.date_trunc("year", F.col("read_at")).alias("read_at_year"),
        F.date_format(F.col("read_at"), "yyyy-MM-dd").alias("read_at_label_2"),
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("read_at")).alias("days_since_read_at_2"),
    )
)

result = (
    scored
    .select("id", "voltage", "voltage_adj")
    .unpivot(
        ids=["id"],
        values=["voltage", "voltage_adj"],
        variableColumnName="measure",
        valueColumnName="value",
    )
    .orderBy("id", "measure")
)
