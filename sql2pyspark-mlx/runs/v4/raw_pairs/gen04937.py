from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status", "carrier").orderBy("cost", "id").rowsBetween(-6, -1)
w_recent = Window.partitionBy("carrier").orderBy(F.col("cost").desc(), "id")
w_grp = Window.orderBy("cost")
w4 = Window.partitionBy("status", "order_id").orderBy("delivered_date", "id")
w_grp_2 = Window.partitionBy("carrier")

metrics = (
    df_shipments
    .select(
        "*",
        F.avg("cost").over(w).alias("cost_avg_6_prior"),
        F.ntile(10).over(w_recent).alias("cost_bucket"),
        F.rank().over(w_grp).alias("cost_rank"),
        F.row_number().over(w4).alias("row_seq"),
        F.max("cost").over(w_grp_2).alias("carrier_max_cost"),
    )
)

result = (
    metrics
    .rollup(
        F.when(F.col("carrier_max_cost") >= 106, "HIGH").otherwise("LOW").alias("carrier_max_cost_tier"),
        F.date_trunc("WEEK", "delivered_date").alias("week_start"),
    )
    .agg(
        F.size(F.collect_set("carrier")).alias("num_carriers"),
        F.countDistinct("order_id").alias("distinct_order_ids"),
        F.size(F.collect_set("status")).alias("num_status"),
        F.round(F.avg("cost_bucket"), 2).alias("avg_cost_bucket"),
    )
    .orderBy(F.col("num_carriers").desc_nulls_last())
)
