from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("method").orderBy(F.col("paid_at").asc_nulls_last(), "id")

deduped = (
    df_payments
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
)

result = (
    deduped
    .withColumn("year_start", F.date_trunc("YEAR", "paid_at"))
    .groupingSets(
        [["year_start", "method"], ["year_start"], []],
        "year_start",
        "method",
    )
    .agg(
        F.percentile_approx("amount", 0.5).alias("amount_p50"),
        F.count(F.when(F.col("method") == "paypal", 1)).alias("paypal_count"),
        ((F.unix_timestamp(F.max("paid_at")) - F.unix_timestamp(F.min("paid_at"))) / 60).alias("duration_min"),
        F.max("paid_at").alias("last_paid_at"),
        F.percentile_approx("amount", 0.9).alias("amount_p90"),
        F.grouping_id().alias("grouping_id"),
    )
    .orderBy(F.col("amount_p90").desc_nulls_first())
)
