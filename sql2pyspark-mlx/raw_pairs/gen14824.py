from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("region", "city").orderBy(F.col("opened_date").desc_nulls_last())
w2 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.sale_date").desc(), F.col("c.id").desc())
w3 = Window.partitionBy(F.month("opened_date"), "city")
w4 = Window.partitionBy(F.year("opened_date"), "city")
w5 = Window.partitionBy("city").orderBy("sale_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy(F.date_trunc("YEAR", "sale_date"), "region").orderBy("sale_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_ordered = Window.partitionBy("region").orderBy("sqft_share", "id")
w_recent = Window.partitionBy("city", "region")
w9 = Window.partitionBy("region", "city").orderBy("sale_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w10 = Window.partitionBy("city").orderBy("cumulative_sqft", "id")

deduped = (
    df_stores
    .withColumn("latest_rank", F.rank().over(w))
    .filter(F.col("latest_rank") == 1)
    .select("id", "city", "region", "opened_date", "sqft")
)

with_last = (
    deduped
    .alias("s")
    .join(df_store_sales.alias("c"), (F.col("c.store_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.sale_date"))
)

enriched = (
    with_last
    .select(
        "*",
        F.round((F.col("sqft") / F.sum("sqft").over(w3)), 4).alias("sqft_share"),
        F.sum("sqft").over(w4).alias("opened_date_year_sum_sqft"),
        F.last("sqft", ignorenulls=True).over(w5).alias("last_known_sqft"),
        F.sum("sqft").over(w6).alias("cumulative_sqft"),
    )
)

enriched_stores = (
    enriched
    .select(
        "*",
        F.lead("opened_date_year_sum_sqft").over(w_ordered).alias("next_opened_date_year_sum_sqft"),
        F.lag("sqft_share").over(w_ordered).alias("prev_sqft_share"),
        F.lead("sqft_share").over(w_ordered).alias("next_sqft_share"),
        F.row_number().over(w_ordered).alias("seq_no"),
    )
)

not_flagged = (
    enriched_stores
    .alias("s")
    .join(df_orders.alias("c"), (F.col("c.store_id") == F.col("s.id")) & (F.col("c.order_date") >= F.col("s.opened_date")), "left_anti")
)

kept = (
    not_flagged
    .alias("s")
    .join(df_orders.alias("ch"), (F.col("ch.store_id") == F.col("s.id")) & (F.col("ch.store_id").isin([10, 32, 34])), "left_anti")
)

with_windows = (
    kept
    .select(
        "id",
        "sqft_share",
        "opened_date_year_sum_sqft",
        "next_opened_date_year_sum_sqft",
        "next_sqft_share",
        F.stddev_samp("seq_no").over(w_recent).alias("city_stddev_samp_seq_no"),
        F.sum("last_known_sqft").over(w9).alias("running_last_known_sqft"),
        F.lead("last_known_sqft", 2).over(w10).alias("next_last_known_sqft"),
    )
)

daily = (
    with_windows
    .groupBy("city_stddev_samp_seq_no")
    .agg(
        F.min("sqft_share").alias("min_sqft_share"),
        F.percentile_approx("next_opened_date_year_sum_sqft", 0.9).alias("next_opened_date_year_sum_sqft_p90"),
    )
    .filter(F.col("min_sqft_share") > 0)
)

result = (
    daily
    .select(
        "city_stddev_samp_seq_no",
        "min_sqft_share",
        "next_opened_date_year_sum_sqft_p90",
    )
)
