from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("cost", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("status").orderBy(F.col("cost").asc_nulls_first())
w3 = Window.partitionBy("order_id").orderBy("shipped_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("status").orderBy("shipped_date", "id").rowsBetween(-5, -1)
w5 = Window.partitionBy("order_id", "status")
w6 = Window.orderBy("shipped_date", "id").rowsBetween(-5, Window.currentRow)
w7 = Window.partitionBy("status").orderBy(F.col("cumulative_cost_dense_rank").asc_nulls_first())
w8 = Window.partitionBy("order_id")
w9 = Window.partitionBy("carrier").orderBy("last_known_cost", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_part = Window.orderBy(F.col("cost_share").desc_nulls_first())

cleaned = (
    df_shipments
    .filter((F.col("delivered_date") > F.lit("2024-04-07").cast("date")) & F.col("order_id").isNotNull() & F.month("shipped_date").isin([2, 4, 6]))
    .withColumn("status_carrier", F.concat("status", F.lit(", "), "carrier"))
    .withColumn("status_title", F.initcap(F.trim("status")))
    .withColumn("carrier_filled", F.coalesce("carrier", F.lit("OTHER")))
)

enriched = (
    cleaned
    .withColumn("last_known_cost", F.last("cost", ignorenulls=True).over(w))
    .withColumn("cost_dense_rank", F.dense_rank().over(w2))
)

enriched_shipments = (
    enriched
    .select(
        "*",
        F.sum("cost_dense_rank").over(w3).alias("cumulative_cost_dense_rank"),
        F.avg("cost_dense_rank").over(w4).alias("cost_dense_rank_avg_5_prior"),
        F.round((F.col("cost") / F.sum("cost").over(w5)), 4).alias("cost_share"),
    )
)

enriched_shipments_2 = (
    enriched_shipments
    .select(
        "*",
        F.avg("cumulative_cost_dense_rank").over(w6).alias("cumulative_cost_dense_rank_avg_5"),
        F.cume_dist().over(w7).alias("cumulative_cost_dense_rank_cume_dist"),
        F.avg("cumulative_cost_dense_rank").over(w8).alias("order_id_avg_cumulative_cost_dense_rank"),
        F.sum("cost").over(w9).alias("cumulative_cost"),
        F.cume_dist().over(w_part).alias("cost_share_cume_dist"),
    )
)

result = (
    enriched_shipments_2
    .withColumn("cost_share_cume_dist_tier", F.when(F.col("cost_share_cume_dist") >= 0.76, "LARGE").otherwise("SMALL"))
    .withColumn("year_start", F.date_trunc("YEAR", "delivered_date"))
    .rollup("cost_share_cume_dist_tier", "year_start")
    .agg(
        F.sort_array(F.collect_set("status")).alias("status_list"),
        F.max(F.when(F.col("carrier") == "FedEx", F.col("cost_share"))).alias("fedex_max_cost_share"),
        F.grouping_id().alias("grouping_id"),
    )
)
