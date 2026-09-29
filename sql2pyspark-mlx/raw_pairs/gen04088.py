from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("key").orderBy(F.col("created_at").desc(), "id", "key")

lines = (
    df_carts
    .select(
        "id",
        "created_at",
        "coupon_codes",
        F.explode("attributes").alias("key", "v"),
    )
)

metrics = lines.withColumn("seq_no", F.row_number().over(w))

ranked = (
    metrics
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
    .drop("latest_rank")
)

valid = (
    ranked
    .filter((F.col("created_at") > F.lit("2025-03-01").cast("date")) & F.col("key").isNotNull())
    .select(
        "*",
        F.length("key").alias("key_length"),
        F.col("created_at").cast("date").alias("created_at_day"),
    )
)

result = (
    valid
    .select(
        "id",
        "created_at",
        "coupon_codes",
        "key",
        "seq_no",
        "key_length",
        "created_at_day",
    )
    .orderBy(F.col("key").desc())
)
