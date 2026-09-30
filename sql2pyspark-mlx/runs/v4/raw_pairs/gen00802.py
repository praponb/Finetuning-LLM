from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("method")
w2 = Window.partitionBy("method").orderBy(F.col("paid_at").cast("bigint")).rangeBetween(-604800, Window.currentRow)
w3 = Window.partitionBy("method").orderBy(F.col("amount_whole_whole").desc_nulls_last())

cleaned = (
    df_payments
    .filter((F.col("paid_at") > F.to_timestamp(F.lit("2023-11-07 04:00:00"))) & F.col("order_id").isNotNull())
    .select("id", "method", "paid_at", F.ceil("amount").alias("amount_whole"))
)

valid = (
    cleaned
    .filter(F.col("paid_at") >= F.lit("2023-12-24 17:00:00").cast("timestamp"))
    .withColumn("method_length", F.length("method"))
    .withColumn("method_filled", F.coalesce("method", F.lit("DIRECT")))
)

matched_keys = (
    valid
    .select("method")
    .exceptAll(
        df_payments
        .filter(F.hour("paid_at").between(11, 18))
        .select("method")
    )
)

verified = (
    valid
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.method") == F.col("k.method")), "inner")
    .select("s.*")
)

cleaned_payments = (
    verified
    .filter(F.col("method") == "cash")
    .select(
        "id",
        "method",
        "paid_at",
        "method_filled",
        F.floor("amount_whole").alias("amount_whole_whole"),
    )
)

scored = (
    cleaned_payments
    .withColumn("method_sum_amount_whole_whole", F.sum("amount_whole_whole").over(w))
    .withColumn("amount_whole_whole_avg_7d", F.avg("amount_whole_whole").over(w2))
    .withColumn("amount_whole_whole_dense_rank", F.dense_rank().over(w3))
)

filtered = (
    scored
    .filter(F.to_date("paid_at") < "2023-03-09")
    .select(
        "method_filled",
        "amount_whole_whole",
        "method_sum_amount_whole_whole",
        "amount_whole_whole_avg_7d",
        "amount_whole_whole_dense_rank",
        F.date_trunc("month", "paid_at").alias("paid_at_period"),
    )
)

summary = (
    filtered
    .withColumn("quarter_start", F.date_trunc("QUARTER", "paid_at_period"))
    .withColumn("year_start", F.date_trunc("YEAR", "paid_at_period"))
    .groupingSets(
        [["quarter_start", "year_start"], ["quarter_start"], []],
        "quarter_start",
        "year_start",
    )
    .agg(
        ((F.unix_timestamp(F.max("paid_at_period")) - F.unix_timestamp(F.min("paid_at_period"))) / 60).alias("span_minutes"),
        ((F.unix_timestamp(F.max("paid_at_period")) - F.unix_timestamp(F.min("paid_at_period"))) / 60).alias("duration_min"),
        F.min("paid_at_period").alias("first_paid_at_period"),
    )
)

result = (
    summary
    .select(
        "quarter_start",
        "year_start",
        "span_minutes",
        "duration_min",
        "first_paid_at_period",
    )
    .orderBy(F.col("first_paid_at_period").desc_nulls_last())
)
