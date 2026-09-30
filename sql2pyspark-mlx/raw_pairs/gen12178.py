from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("attr_key").orderBy(F.col("created_at").desc(), "id", "attr_key")
w2 = Window.partitionBy("attr_key").orderBy(F.col("event_seq").asc_nulls_first(), "id", "attr_key")
w3 = Window.partitionBy("attr_key").orderBy("created_at", "id", "attr_key")
w4 = Window.partitionBy("attr_key").orderBy("created_at", "id", "attr_key").rowsBetween(Window.unboundedPreceding, Window.currentRow)

exploded = (
    df_carts
    .select(
        "id",
        "customer_id",
        "created_at",
        "coupon_codes",
        F.explode("attributes").alias("attr_key", "attr_value"),
    )
)

base = (
    exploded
    .filter(F.col("created_at") > F.to_date(F.lit("2025-03-01")))
    .select(
        "id",
        "created_at",
        "attr_key",
        "attr_value",
        F.coalesce("attr_key", F.lit("unassigned")).alias("attr_key_filled"),
        F.coalesce("attr_key", F.lit("OTHER")).alias("attr_key_clean"),
    )
)

with_windows = (
    base
    .filter(F.col("attr_key").isin(["device", "weight_kg"]))
    .withColumn("event_seq", F.row_number().over(w))
)

ranked = (
    with_windows
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

with_gaps = (
    ranked
    .withColumn("is_new_burst", F.when(F.lag("created_at").over(w3).isNull() | (F.col("created_at") > F.lag("created_at").over(w3) + F.expr("INTERVAL 15 MINUTES")), 1).otherwise(0))
)

islands = (
    with_gaps
    .select(
        "id",
        "created_at",
        "attr_key",
        "attr_key_clean",
        "event_seq",
        F.sum("is_new_burst").over(w4).alias("session_no"),
    )
)

result = (
    islands
    .select(
        "id",
        "created_at",
        "attr_key",
        "attr_key_clean",
        "event_seq",
        "session_no",
        F.when(F.col("attr_key").rlike("^(device|gift)$"), "EXTENDED").otherwise("GROUP_A").alias("attr_key_zone"),
    )
)
