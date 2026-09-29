from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("sqft", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy("sqft", "id").rowsBetween(-6, Window.currentRow)
w3 = Window.orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("city").orderBy(F.col("opened_date").desc(), "id")
w5 = Window.partitionBy("city").orderBy(F.col("sqft_avg_6").asc_nulls_first(), "id")
w6 = Window.orderBy(F.col("sqft").asc_nulls_first())
w7 = Window.orderBy("sqft_avg_6", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.orderBy("sqft_avg_6", "id")
w9 = Window.orderBy("running_sqft", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w10 = Window.orderBy(F.col("sqft").desc(), "id")
w11 = Window.orderBy("sqft_bucket", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

enriched = (
    df_stores
    .filter(~F.col("city").isin(["Singapore", "Chiang Mai"]))
    .withColumn("running_sqft", F.sum("sqft").over(w))
    .withColumn("sqft_avg_6", F.avg("sqft").over(w2))
    .withColumn("cumulative_sqft", F.sum("sqft").over(w3))
    .withColumn("seq_no", F.row_number().over(w4))
)

ranked = (
    enriched
    .withColumn("rank_in_group", F.row_number().over(w5))
    .filter(F.col("rank_in_group") == 1)
    .select(
        "id",
        "opened_date",
        "sqft",
        "running_sqft",
        "sqft_avg_6",
        "cumulative_sqft",
        "seq_no",
    )
)

metrics = (
    ranked
    .select(
        "id",
        "opened_date",
        "sqft",
        "cumulative_sqft",
        "seq_no",
        F.percent_rank().over(w6).alias("sqft_percent_rank"),
        F.last("sqft_avg_6", ignorenulls=True).over(w7).alias("last_known_sqft_avg_6"),
        F.lead("running_sqft", 1).over(w8).alias("next_running_sqft"),
        F.sum("seq_no").over(w9).alias("cumulative_seq_no"),
        F.ntile(5).over(w10).alias("sqft_bucket"),
    )
)

eligible = (
    metrics
    .alias("s")
    .join(df_orders.alias("ch"), (F.col("ch.store_id") == F.col("s.id")) & (F.col("ch.customer_id").isNotNull()), "left_anti")
)

not_flagged = (
    eligible
    .alias("s")
    .join(df_inventory.alias("x"), (F.col("x.store_id") == F.col("s.id")) & (F.col("x.quantity").between(165, 180)), "left_semi")
)

eligible_stores = (
    not_flagged
    .alias("s")
    .join(df_orders.alias("x"), (F.col("x.store_id") == F.col("s.id")) & (F.col("x.order_date") < F.col("s.opened_date")), "left_semi")
)

kept = (
    eligible_stores
    .alias("s")
    .join(df_store_sales.alias("c"), (F.col("c.store_id") == F.col("s.id")) & (F.col("c.sale_date") < F.col("s.opened_date")), "left_anti")
)

result = (
    kept
    .select(
        "id",
        "opened_date",
        "cumulative_sqft",
        "seq_no",
        "last_known_sqft_avg_6",
        "next_running_sqft",
        "cumulative_seq_no",
        F.count("next_running_sqft").over(w11).alias("running_count_next_running_sqft"),
        F.avg("next_running_sqft").over(w11).alias("running_avg_next_running_sqft"),
        F.min("next_running_sqft").over(w11).alias("running_min_next_running_sqft"),
        F.max("next_running_sqft").over(w11).alias("running_max_next_running_sqft"),
    )
)
