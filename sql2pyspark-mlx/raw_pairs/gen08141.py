from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.amount").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("status").orderBy("sqft", "id")
w3 = Window.partitionBy("status").orderBy("opened_date", "id").rowsBetween(-6, -1)
w4 = Window.partitionBy(F.year("opened_date"), "status").orderBy("opened_date", "id").rowsBetween(-6, -1)
w5 = Window.partitionBy(F.date_trunc("month", "opened_date"), "status")

valid = (
    df_stores
    .filter((F.col("opened_date") > F.lit("2023-09-11").cast("date")) & F.col("region").isNotNull())
    .select(
        "id",
        "store_name",
        "opened_date",
        "sqft",
        F.coalesce("store_name", F.lit("Unknown")).alias("store_name_filled"),
        F.coalesce("region", F.lit("DIRECT")).alias("resolved_region"),
    )
)

with_latest = (
    valid
    .alias("s")
    .join(df_orders.alias("c"), (F.col("c.store_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.store_name",
        "s.opened_date",
        "s.sqft",
        "s.resolved_region",
        F.col("c.status"),
    )
)

metrics = (
    with_latest
    .withColumn("seq_no", F.row_number().over(w2))
    .withColumn("sqft_avg_6_prior", F.avg("sqft").over(w3))
    .withColumn("sqft_avg_6_prior_2", F.avg("sqft").over(w4))
    .withColumn("opened_date_period_avg_sqft", F.avg("sqft").over(w5))
)

cleaned = (
    metrics
    .filter(F.col("sqft") == 9672)
    .select(
        "id",
        "store_name",
        "sqft",
        "resolved_region",
        "status",
        "seq_no",
        F.round("sqft_avg_6_prior_2", 0).alias("sqft_avg_6_prior_2_rounded"),
        F.datediff(F.current_date(), "opened_date").alias("days_since_opened_date"),
    )
)

result = (
    cleaned
    .groupBy(
        F.when(F.col("sqft_avg_6_prior_2_rounded") >= 83, "LARGE").otherwise("SMALL").alias("sqft_avg_6_prior_2_rounded_tier"),
        F.when(F.col("sqft") >= 13080, "HIGH").otherwise("LOW").alias("sqft_tier"),
    )
    .agg(
        F.bit_and("sqft").alias("sqft_bit_and"),
        F.countDistinct("id").alias("distinct_ids"),
        F.round(F.sum("sqft_avg_6_prior_2_rounded") / F.nullif(F.count(F.when(F.col("status") == "COMPLETED", 1)), F.lit(0)), 2).alias("sqft_avg_6_prior_2_rounded_per_completed"),
    )
    .orderBy("sqft_avg_6_prior_2_rounded_tier")
)
