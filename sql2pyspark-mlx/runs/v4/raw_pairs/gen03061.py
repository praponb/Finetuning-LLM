from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("cost").desc())
w2 = Window.partitionBy("status")
cost_by_order_id = df_shipments.groupBy("order_id").agg(F.sum("cost").alias("sum_shipment_cost"))

kept_keys = (
    df_shipments
    .select("status")
    .subtract(
        df_shipments
        .filter(F.col("carrier") == "DHL")
        .select("status")
    )
)

reconciled = (
    df_shipments
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.status") == F.col("k.status")), "inner")
    .select("s.*")
)

joined = (
    reconciled
    .alias("src")
    .join(df_orders.alias("p"), F.col("src.order_id") == F.col("p.id"), "left")
    .select("src.*", F.col("p.channel"))
)

scored = (
    joined
    .withColumn("cost_rank", F.rank().over(w))
    .withColumn("status_count_cost", F.count("cost").over(w2))
    .withColumn("cost_cume_dist", F.cume_dist().over(w))
)

base = (
    scored
    .filter(F.col("delivered_date") >= F.to_date(F.lit("2023-06-18")))
    .select(
        "*",
        F.round((F.col("status_count_cost") / F.col("cost")), 2).alias("status_count_cost_per_cost"),
        F.date_add("delivered_date", 60).alias("delivered_date_plus_60d"),
    )
)

grouped = (
    base
    .groupBy("order_id")
    .agg(
        F.avg(F.when(F.col("carrier") == "FedEx", F.col("status_count_cost_per_cost"))).alias("fedex_avg_status_count_cost_per_cost"),
        F.round(F.sum("status_count_cost_per_cost") / F.nullif(F.count(F.when(F.col("channel") == "mobile", 1)), F.lit(0)), 2).alias("status_count_cost_per_cost_per_mobile"),
        F.max(F.when(F.col("carrier") == "UPS", F.col("status_count_cost"))).alias("ups_max_status_count_cost"),
    )
    .filter(F.col("ups_max_status_count_cost") >= 0)
)

result = (
    grouped
    .join(cost_by_order_id, on="order_id", how="left")
    .select(
        "order_id",
        "status_count_cost_per_cost_per_mobile",
        "ups_max_status_count_cost",
        F.col("sum_shipment_cost"),
    )
)
