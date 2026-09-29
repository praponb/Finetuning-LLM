from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel", "promo_code").orderBy(F.col("discount_pct").desc(), "id")
w2 = Window.orderBy("adjusted_discount_pct", "id")
w3 = Window.orderBy(F.col("adjusted_discount_pct").desc())
w4 = Window.orderBy(F.col("adjusted_discount_pct").desc_nulls_first())

cleaned = (
    df_promotions
    .filter(F.col("ends_on") >= "2025-05-01")
    .select(
        "id",
        "channel",
        "promo_code",
        "starts_on",
        "discount_pct",
        F.round((F.col("discount_pct") * 1.1), 2).alias("adjusted_discount_pct"),
        F.col("discount_pct").cast("int").alias("discount_pct_int"),
        F.coalesce("channel", F.lit("DIRECT")).alias("channel_clean"),
    )
)

deduped = (
    cleaned
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
    .select("id", "starts_on", "discount_pct", "adjusted_discount_pct")
)

valid = (
    deduped
    .filter(F.col("adjusted_discount_pct") <= 40)
    .withColumn("starts_on_minus_71d", F.date_sub("starts_on", 71))
)

filtered = (
    valid
    .filter(F.col("starts_on") >= F.lit("2023-07-20").cast("date"))
    .select(
        "id",
        "discount_pct",
        "adjusted_discount_pct",
        "starts_on_minus_71d",
        F.when(F.col("adjusted_discount_pct") >= 80, "High").when(F.col("adjusted_discount_pct") >= 28, "Medium").otherwise("Low").alias("adjusted_discount_pct_level"),
        (F.datediff("starts_on", "starts_on_minus_71d") / 7).alias("weeks_starts_on_minus_71d_to_starts_on"),
    )
)

metrics = (
    filtered
    .withColumn("next_adjusted_discount_pct", F.lead("adjusted_discount_pct", 1).over(w2))
    .withColumn("adjusted_discount_pct_cume_dist", F.cume_dist().over(w3))
    .withColumn("adjusted_discount_pct_percent_rank", F.percent_rank().over(w4))
)

result = (
    metrics
    .select(
        "id",
        F.col("next_adjusted_discount_pct").alias("NEXT_ADJUSTED_DISCOUNT_PCT_VAL"),
        F.col("discount_pct").alias("DISCOUNT_PCT"),
        F.col("adjusted_discount_pct_cume_dist").alias("ADJUSTED_DISCOUNT_PCT_CUME_DIST"),
    )
    .unpivot(
        ids=["id"],
        values=["NEXT_ADJUSTED_DISCOUNT_PCT_VAL", "DISCOUNT_PCT", "ADJUSTED_DISCOUNT_PCT_CUME_DIST"],
        variableColumnName="metric",
        valueColumnName="reading",
    )
    .filter(F.col("reading").isNotNull())
)
