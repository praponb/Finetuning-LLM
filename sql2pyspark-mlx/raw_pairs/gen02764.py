from pyspark.sql import functions as F

kept_keys = (
    df_shipments
    .select("order_id")
    .intersect(
        df_shipments
        .filter(F.col("order_id").isin([2, 35]))
        .select("order_id")
    )
)

verified = (
    df_shipments
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.order_id") == F.col("k.order_id")), "inner")
    .select("s.*")
)

result = (
    verified
    .groupBy(F.date_trunc("QUARTER", "shipped_date").alias("quarter_start"))
    .agg(
        F.count_if(F.col("carrier") == "UPS").alias("ups_events"),
        F.round(F.sum("cost") / F.nullif(F.count(F.when(F.col("status") == "LOST", 1)), F.lit(0)), 2).alias("cost_per_lost"),
    )
    .filter(F.col("ups_events") > 1)
    .orderBy(F.col("ups_events").asc_nulls_first())
)
