from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city", "region").orderBy(F.col("sqft").desc_nulls_first())
w2 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.units").desc(), F.col("c.id").desc())
w3 = Window.partitionBy(F.year("sale_date"), "region").orderBy("opened_date", "id")
w4 = Window.partitionBy("region", "city").orderBy(F.col("opened_date").desc(), "id")
w5 = Window.partitionBy("region").orderBy("store_sale_units", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy(F.month("opened_date"), "region")
w7 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.sale_date").desc(), F.col("c.id").desc())
w8 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.amount").desc(), F.col("c.id").desc())
w9 = Window.orderBy("amount", "id").rowsBetween(-3, -1)
w_recent = Window.orderBy("amount", "id")

ranked = (
    df_stores
    .withColumn("dedup_rank", F.rank().over(w))
    .filter(F.col("dedup_rank") == 1)
)

latest_match = (
    ranked
    .alias("s")
    .join(df_store_sales.alias("c"), (F.col("c.store_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.sale_date"), F.col("c.units").alias("store_sale_units"))
)

enriched = (
    latest_match
    .filter(F.col("opened_date") >= "2025-10-18")
    .select(
        "id",
        "store_name",
        "city",
        "dedup_rank",
        "store_sale_units",
        F.lag("store_sale_units", 1).over(w3).alias("prev_store_sale_units"),
        F.row_number().over(w4).alias("event_seq"),
        F.sum("sqft").over(w5).alias("running_sqft"),
        F.stddev_samp("sqft").over(w6).alias("opened_date_month_stddev_samp_sqft"),
    )
)

with_last = (
    enriched
    .alias("s")
    .join(df_store_sales.alias("c"), (F.col("c.store_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w7))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.dedup_rank",
        "s.store_sale_units",
        "s.prev_store_sale_units",
        "s.event_seq",
        "s.running_sqft",
        F.col("c.units"),
        F.col("c.revenue").alias("store_sale_revenue"),
    )
)

with_latest = (
    with_last
    .alias("s")
    .join(df_orders.alias("c"), (F.col("c.store_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w8))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.prev_store_sale_units",
        "s.units",
        F.col("c.order_date").alias("order_order_date"),
        F.col("c.amount"),
    )
)

metrics = (
    with_latest
    .select(
        "*",
        F.avg("amount").over(w9).alias("amount_avg_3_prior"),
        F.lag("units", 1, 0).over(w_recent).alias("prev_units"),
    )
)

not_flagged = (
    metrics
    .alias("s")
    .join(df_store_sales.alias("c"), (F.col("c.store_id") == F.col("s.id")) & (F.col("c.store_id") == 3), "left_anti")
)

result = (
    not_flagged
    .groupBy(
        F.when(F.col("amount_avg_3_prior") >= 1412, "LARGE").otherwise("SMALL").alias("amount_avg_3_prior_tier"),
    )
    .agg(
        F.count("*").alias("row_count"),
        F.round(F.avg("units"), 2).alias("avg_units"),
    )
    .orderBy(F.col("row_count").desc())
)
