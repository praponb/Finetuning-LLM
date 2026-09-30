from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy("opened_date", "id")
w2 = Window.partitionBy("city", "region")
w_part = Window.partitionBy("city").orderBy("city_min_sqft", "id").rowsBetween(-5, -1)
w_ordered = Window.partitionBy("city").orderBy("prev_sqft", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("city")
w6 = Window.partitionBy("city").orderBy("city_min_sqft_share", "id")
w7 = Window.partitionBy("city").orderBy(F.col("cumulative_prev_sqft").desc(), "id")
w8 = Window.partitionBy("city").orderBy("row_seq", "id").rowsBetween(-6, -1)
w9 = Window.partitionBy("city").orderBy(F.col("prev_sqft_share").desc(), "id")

with_windows = (
    df_stores
    .select(
        "id",
        "store_name",
        "city",
        "sqft",
        F.lag("sqft").over(w).alias("prev_sqft"),
        F.row_number().over(w).alias("row_seq"),
        F.min("sqft").over(w2).alias("city_min_sqft"),
    )
)

scored = (
    with_windows
    .filter(F.col("city").isin(["Chiang Mai", "Berlin", "Singapore"]))
    .select(
        "*",
        F.avg("sqft").over(w_part).alias("sqft_avg_5_prior"),
        F.sum("prev_sqft").over(w_ordered).alias("cumulative_prev_sqft"),
        F.round((F.col("city_min_sqft") / F.sum("city_min_sqft").over(w5)), 4).alias("city_min_sqft_share"),
        F.round((F.col("prev_sqft") / F.sum("prev_sqft").over(w5)), 4).alias("prev_sqft_share"),
    )
)

kept = (
    scored
    .alias("s")
    .join(df_inventory.alias("c"), (F.col("c.store_id") == F.col("s.id")) & (F.col("c.quantity") > 172), "left_anti")
)

metrics = (
    kept
    .withColumn("prev_city_min_sqft_share", F.lag("city_min_sqft_share", 1).over(w6))
    .withColumn("seq_no", F.row_number().over(w7))
    .withColumn("sqft_avg_6_prior", F.avg("sqft").over(w8))
    .withColumn("prev_sqft_share_bucket", F.ntile(4).over(w9))
)

daily = (
    metrics
    .withColumn("sqft_tier", F.when(F.col("sqft") >= 16135, "PREMIUM").otherwise("STANDARD"))
    .groupingSets(
        [["city", "sqft_tier", "seq_no"], ["city", "sqft_tier"], ["city"], []],
        "city",
        "sqft_tier",
        "seq_no",
    )
    .agg(
        F.bit_and("sqft").alias("sqft_bit_and"),
        F.countDistinct("id").alias("distinct_ids"),
    )
    .filter(F.col("sqft_bit_and") > 1)
)

result = (
    daily
    .select(
        "city",
        "sqft_tier",
        "sqft_bit_and",
        "distinct_ids",
        F.when(F.col("city").rlike("^(London|Chiang Mai)$"), "PRIORITY").when(F.col("city").rlike("^(Bangkok|New York)$"), "GROUP_A").otherwise("GROUP_B").alias("city_zone"),
    )
    .orderBy(F.col("sqft_bit_and").desc())
)
