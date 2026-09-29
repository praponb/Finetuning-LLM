from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy(F.col("temp_c").asc_nulls_last(), "id")
w2 = Window.partitionBy("device_code").orderBy(F.col("humidity").desc_nulls_first(), "id")
w3 = Window.orderBy("temp_c", "id")
w4 = Window.orderBy("temp_c", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.orderBy(F.col("reading_ts").cast("bigint")).rangeBetween(-3600, Window.currentRow)

ranked = (
    df_device_readings
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
    .drop("dedup_rank")
)

deduped = (
    ranked
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select("id", "reading_ts", "temp_c")
)

enriched = (
    deduped
    .select(
        "*",
        F.lead("temp_c", 1).over(w3).alias("next_temp_c"),
        F.last("temp_c", ignorenulls=True).over(w4).alias("last_known_temp_c"),
    )
)

remaining_keys = (
    enriched
    .select("id")
    .subtract(
        df_device_readings
        .filter(F.col("status_bits") <= 5)
        .select("id")
    )
)

verified = (
    enriched
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

scored = (
    verified
    .select(
        "id",
        "reading_ts",
        "temp_c",
        "last_known_temp_c",
        F.sum("last_known_temp_c").over(w5).alias("last_known_temp_c_sum_1h"),
    )
)

valid = (
    scored
    .filter(F.col("last_known_temp_c_sum_1h") > 86)
    .withColumn("last_known_temp_c_sum_1h_scaled", F.round((F.col("last_known_temp_c_sum_1h") * 100), 2))
    .withColumn("last_known_temp_c_sum_1h_band", F.when(F.col("last_known_temp_c_sum_1h") >= 155, "High").when(F.col("last_known_temp_c_sum_1h") >= 46, "Medium").otherwise("Low"))
)

result = (
    valid
    .select("id", "reading_ts", "temp_c", "last_known_temp_c")
    .orderBy("id")
)
