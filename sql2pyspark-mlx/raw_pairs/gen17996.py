from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.discount").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("city").orderBy("sqft", "id")
w_ordered = Window.partitionBy("city")
w_recent = Window.partitionBy("city").orderBy(F.col("sqft").desc(), "id")
w5 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.units").desc(), F.col("c.id").desc())

valid = (
    df_stores
    .filter(F.col("region").isNotNull())
    .select("*", F.upper("store_name").alias("store_name_upper"))
)

with_last = (
    valid
    .alias("s")
    .join(df_orders.alias("c"), (F.col("c.store_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.status").alias("order_status"), F.col("c.amount"))
)

filtered = (
    with_last
    .filter((F.col("opened_date") >= F.lit("2023-08-09").cast("date")) & (F.col("sqft") < 7526))
    .withColumn("sqft_whole", F.ceil("sqft"))
    .withColumn("store_name_lower", F.lower("store_name"))
)

ranked = (
    filtered
    .withColumn("rank_in_group", F.row_number().over(w2))
    .filter(F.col("rank_in_group") == 1)
    .select(
        "id",
        "store_name",
        "city",
        "opened_date",
        "sqft",
        "amount",
        "sqft_whole",
        "store_name_lower",
    )
)

scored = (
    ranked
    .withColumn("city_max_sqft_whole", F.max("sqft_whole").over(w_ordered))
    .withColumn("seq_no", F.row_number().over(w_recent))
)

with_latest = (
    scored
    .alias("s")
    .join(df_store_sales.alias("c"), (F.col("c.store_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w5))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.city",
        "s.opened_date",
        "s.sqft",
        "s.amount",
        "s.city_max_sqft_whole",
        "s.seq_no",
        F.col("c.revenue").alias("store_sale_revenue"),
    )
)

result = (
    with_latest
    .groupBy(F.date_trunc("MONTH", "opened_date").alias("activity_month"))
    .agg(
        F.min("amount").alias("min_amount"),
        F.count("*").alias("num_rows"),
        F.percentile_approx("seq_no", 0.9).alias("seq_no_p90"),
        F.count("*").alias("num_rows_2"),
        F.count("*").alias("event_count"),
        F.bit_or("sqft").alias("sqft_bit_or"),
    )
)
