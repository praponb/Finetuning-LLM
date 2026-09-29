from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel").orderBy("days_since_ends_on", "id")
w2 = Window.partitionBy("channel").orderBy(F.col("row_num").desc())
w3 = Window.orderBy(F.col("ends_on").desc(), "id")
w4 = Window.orderBy("ends_on", "id")

valid = (
    df_promotions
    .filter(F.col("starts_on") >= "2024-02-01")
    .select(
        "id",
        "channel",
        "ends_on",
        F.datediff(F.current_date(), "ends_on").alias("days_since_ends_on"),
        F.lower("promo_code").alias("promo_code_lower"),
        F.coalesce("channel", F.lit("OTHER")).alias("resolved_channel"),
    )
)

deduped = (
    valid
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
)

deduped_promotions = (
    deduped
    .withColumn("row_num_2", F.rank().over(w2))
    .filter(F.col("row_num_2") == 1)
    .select("id", "ends_on", "promo_code_lower", "resolved_channel")
)

metrics = (
    deduped_promotions
    .filter(F.col("ends_on") >= "2025-07-06")
    .select(
        "id",
        "promo_code_lower",
        "resolved_channel",
        F.row_number().over(w3).alias("seq_no"),
        F.row_number().over(w4).alias("seq_no_2"),
        F.row_number().over(w4).alias("row_seq"),
    )
)

cleaned = (
    metrics
    .filter(F.col("row_seq") > 3)
    .select(
        "id",
        "resolved_channel",
        "seq_no",
        "seq_no_2",
        "row_seq",
        (F.col("row_seq") * F.col("seq_no_2")).alias("row_seq_times_seq_no_2"),
    )
)

result = (
    cleaned
    .select("id", "seq_no", "seq_no_2", "row_seq")
    .orderBy(F.col("seq_no_2").desc())
)
