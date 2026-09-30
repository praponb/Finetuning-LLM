from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy(F.col("device_length").desc(), "id")
w2 = Window.partitionBy("customer_id").orderBy(F.col("device_length").desc_nulls_first(), "id")
w_part = Window.partitionBy("device").orderBy("device_length", "id").rowsBetween(-2, -1)
w4 = Window.orderBy("device_length", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_sessions
    .filter(F.col("country").isNotNull() & (F.col("country") == "TH"))
    .withColumn("device_length", F.length("device"))
    .withColumn("resolved_country", F.coalesce("country", F.lit("OTHER")))
)

latest = (
    cleaned
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "id",
        "customer_id",
        "device",
        "country",
        "device_length",
        "resolved_country",
    )
)

filtered = (
    latest
    .filter(~F.col("country").eqNullSafe(F.lit("SG")))
    .select(
        "id",
        "customer_id",
        "device",
        "device_length",
        "resolved_country",
        F.lower("country").alias("country_lower"),
    )
)

latest_sessions = (
    filtered
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

metrics = (
    latest_sessions
    .select(
        "id",
        "device_length",
        "resolved_country",
        "country_lower",
        F.avg("device_length").over(w_part).alias("device_length_avg_2_prior"),
        F.first("device_length", ignorenulls=True).over(w4).alias("first_known_device_length"),
    )
)

valid = (
    metrics
    .filter(F.col("device_length") >= 35)
    .withColumn("adjusted_device_length_avg_2_prior", F.round((F.col("device_length_avg_2_prior") * 100), 2))
    .withColumn("first_known_device_length_adjusted", F.round((F.col("first_known_device_length") * 100), 2))
)

eligible = (
    valid
    .alias("s")
    .join(df_web_events.alias("x"), (F.col("x.session_id") == F.col("s.id")) & (F.col("x.page") == "/product"), "left_semi")
)

result = (
    eligible
    .select(
        "id",
        "device_length",
        "country_lower",
        "device_length_avg_2_prior",
        "first_known_device_length",
        "adjusted_device_length_avg_2_prior",
    )
)
