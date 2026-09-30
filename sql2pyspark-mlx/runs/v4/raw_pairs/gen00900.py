from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.partitionBy("country").orderBy(F.col("started_at").desc(), "id")
w_recent = Window.orderBy(F.col("started_at").cast("bigint")).rangeBetween(-7776000, Window.currentRow)
w3 = Window.orderBy("started_at_hour", "id")
w4 = Window.partitionBy("country").orderBy(F.col("started_at_hour").desc(), "id")
w5 = Window.partitionBy("customer_id").orderBy("started_at_hour_count_90d", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("customer_id", "browser").orderBy(F.col("started_at").cast("bigint")).rangeBetween(-7776000, Window.currentRow)
w7 = Window.partitionBy("browser")
w8 = Window.partitionBy("country", "customer_id").orderBy(F.col("prev_started_at_hour_count_90d").desc_nulls_last(), "id")
w_ordered = Window.partitionBy("browser").orderBy(F.col("started_at_hour").desc())
w10 = Window.orderBy("signup_date", "id")

valid = (
    df_sessions
    .filter((F.col("started_at") >= F.lit("2023-10-16").cast("date")) & F.col("country").isNotNull() & ~F.col("device").isin(["tablet", "desktop", "mobile"]))
    .select(
        "id",
        "customer_id",
        "browser",
        "started_at",
        "country",
        F.hour("started_at").alias("started_at_hour"),
        F.coalesce("browser", F.lit("N/A")).alias("browser_filled"),
    )
)

metrics = (
    valid
    .withColumn("seq_no", F.row_number().over(w_part))
    .withColumn("started_at_hour_count_90d", F.count("started_at_hour").over(w_recent))
)

scored = (
    metrics
    .select(
        "*",
        F.lag("started_at_hour_count_90d", 1).over(w3).alias("prev_started_at_hour_count_90d"),
        F.row_number().over(w4).alias("row_seq"),
        F.first("browser", ignorenulls=True).over(w5).alias("first_known_browser"),
        F.count("started_at_hour").over(w6).alias("started_at_hour_count_90d_2"),
        F.round((F.col("started_at_hour_count_90d") / F.sum("started_at_hour_count_90d").over(w7)), 4).alias("started_at_hour_count_90d_share"),
    )
)

ranked = (
    scored
    .withColumn("rn", F.row_number().over(w8))
    .filter(F.col("rn") == 1)
)

joined = (
    ranked
    .alias("src")
    .join(df_customers.alias("d"), F.col("src.customer_id") == F.col("d.id"), "left")
    .select("src.*", F.col("d.signup_date"))
)

with_windows = (
    joined
    .select(
        "*",
        F.percent_rank().over(w_ordered).alias("started_at_hour_percent_rank"),
        F.lag("row_seq", 2).over(w10).alias("prev_row_seq"),
    )
)

stats = (
    with_windows
    .cube("seq_no")
    .agg(
        F.sum(F.when(F.col("browser") == "Firefox", F.col("seq_no")).otherwise(0)).alias("firefox_seq_no"),
        F.round(F.sum("started_at_hour_percent_rank") / F.nullif(F.count(F.when(F.col("country") == "SG", 1)), F.lit(0)), 2).alias("started_at_hour_percent_rank_per_sg"),
        F.grouping("seq_no").alias("grouping_id"),
    )
    .filter(F.col("firefox_seq_no") > 0)
)

result = (
    stats
    .select("seq_no", "started_at_hour_percent_rank_per_sg", "grouping_id")
    .orderBy(F.col("seq_no").desc())
)
