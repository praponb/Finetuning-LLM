from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.partitionBy("ship_country").orderBy("created_at", "id")
w2 = Window.partitionBy(F.month("created_at"), "ship_country").orderBy("created_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy(F.date_trunc("YEAR", "created_at"), "ship_country").orderBy(F.col("created_at").desc(), "id")
w_running = Window.partitionBy("ship_country").orderBy(F.col("created_at").desc(), "id")

unpacked = (
    df_carts
    .select(
        "id",
        "created_at",
        "ship_to",
        F.coalesce(F.col("attributes")["gift"], F.lit("unknown")).alias("gift_or_default"),
        F.coalesce(F.col("attributes").getItem("utm_source"), F.lit("unknown")).alias("utm_source_or_default"),
        F.col("ship_to.country").alias("ship_country"),
        F.coalesce(F.col("ship_to.city"), F.lit("UNKNOWN")).alias("city_or_unknown"),
        F.array_contains(F.map_keys("attributes"), "weight_kg").alias("has_weight_kg"),
    )
)

result = (
    unpacked
    .withColumn("row_seq", F.row_number().over(w_ordered))
    .withColumn("seq_no", F.row_number().over(w_ordered))
    .withColumn("last_known_gift_or_default", F.last("gift_or_default", ignorenulls=True).over(w2))
    .withColumn("event_seq", F.row_number().over(w3))
    .withColumn("row_seq_2", F.row_number().over(w_running))
)
