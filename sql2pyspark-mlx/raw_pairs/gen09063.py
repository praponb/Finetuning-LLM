from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.year("order_date")).orderBy(F.col("discount").desc())
w2 = Window.orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("customer_id", "store_id").orderBy(F.col("discount_rank").asc_nulls_first(), "id")
w4 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.amount").desc(), F.col("c.id").desc())

with_windows = (
    df_orders
    .filter(F.col("order_date") > "2024-09-14")
    .select(
        "id",
        "customer_id",
        "store_id",
        "amount",
        F.rank().over(w).alias("discount_rank"),
        F.first("amount", ignorenulls=True).over(w2).alias("first_known_amount"),
    )
)

latest = (
    with_windows
    .withColumn("rank_in_group", F.row_number().over(w3))
    .filter(F.col("rank_in_group") == 1)
)

with_dims = (
    latest
    .alias("s")
    .join(df_stores.alias("p"), F.col("s.store_id").eqNullSafe(F.col("p.id")), "inner")
    .select(
        "s.id",
        "s.store_id",
        "s.discount_rank",
        "s.first_known_amount",
        "s.rank_in_group",
        F.col("p.sqft"),
    )
)

with_last = (
    with_dims
    .alias("s")
    .join(df_payments.alias("c"), (F.col("c.order_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w4))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.store_id",
        "s.discount_rank",
        "s.first_known_amount",
        "s.rank_in_group",
        "s.sqft",
        F.col("c.amount"),
        F.col("c.status"),
    )
)

joined = (
    with_last
    .alias("e")
    .join(df_stores.alias("s"), F.col("e.store_id") == F.col("s.id"), "left")
    .select(
        "e.id",
        "e.store_id",
        "e.rank_in_group",
        "e.sqft",
        "e.amount",
        "e.status",
        F.col("s.sqft").alias("store_sqft"),
        F.col("s.city"),
        F.col("s.opened_date"),
    )
)

result = (
    joined
    .groupBy(
        F.when(F.col("amount") >= 1370, "HIGH").otherwise("LOW").alias("amount_tier"),
    )
    .agg(
        F.percentile_approx("store_sqft", 0.5).alias("store_sqft_p50"),
        F.round(F.avg("sqft"), 2).alias("avg_sqft"),
        F.sort_array(F.collect_set("city")).alias("city_list"),
        F.count(F.when(F.col("city") == "Tokyo", 1)).alias("tokyo_count"),
        F.count(F.when(F.col("city") == "New York", 1)).alias("new_york_count"),
        F.size(F.collect_set("status")).alias("num_status"),
    )
)
