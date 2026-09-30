from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id")
w2 = Window.partitionBy(F.month("order_date"), "store_id").orderBy("shipping_fee", "id").rowsBetween(-3, Window.currentRow)
w3 = Window.partitionBy("customer_id").orderBy("order_date", "id").rowsBetween(-2, Window.currentRow)
w4 = Window.partitionBy("channel")
quantity_by_store_id = df_inventory.groupBy("store_id").agg(F.sum("quantity").alias("sum_inventory_quantity"))

valid = (
    df_orders
    .filter(F.col("order_date") >= F.lit("2023-07-05").cast("date"))
    .select(
        "*",
        F.datediff(F.current_date(), "order_date").alias("days_since_order_date"),
    )
)

with_windows = (
    valid
    .filter(F.col("store_id").isNotNull())
    .select(
        "id",
        "customer_id",
        "store_id",
        "discount",
        "days_since_order_date",
        F.max("shipping_fee").over(w).alias("store_id_max_shipping_fee"),
        F.avg("days_since_order_date").over(w2).alias("days_since_order_date_avg_3"),
        F.avg("amount").over(w3).alias("amount_avg_2"),
        F.avg("shipping_fee").over(w4).alias("channel_avg_shipping_fee"),
    )
)

with_dims = (
    with_windows
    .alias("s")
    .join(df_stores.alias("p"), F.col("s.store_id").eqNullSafe(F.col("p.id")), "left")
    .select(
        "s.id",
        "s.customer_id",
        "s.store_id",
        "s.days_since_order_date_avg_3",
        F.col("p.sqft"),
        F.col("p.opened_date"),
        F.col("p.store_name"),
    )
)

matched_keys = (
    with_dims
    .select("store_id", "customer_id")
    .subtract(
        df_orders
        .filter(F.col("order_date") <= "2023-05-01")
        .select("store_id", "customer_id")
    )
)

verified = (
    with_dims
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.store_id") == F.col("k.store_id")) & (F.col("s.customer_id") == F.col("k.customer_id")), "inner")
    .select("s.*")
)

stats = (
    verified
    .groupBy("store_id", F.date_trunc("MONTH", "opened_date").alias("activity_month"))
    .agg(
        F.min("sqft").alias("min_sqft"),
        F.round(F.avg("sqft"), 2).alias("avg_sqft"),
        F.countDistinct("store_id").alias("distinct_store_ids"),
        F.countDistinct("id").alias("distinct_ids"),
        F.percentile_approx("days_since_order_date_avg_3", 0.9).alias("days_since_order_date_avg_3_p90"),
    )
)

result = (
    stats
    .join(quantity_by_store_id, on="store_id", how="left")
    .select(
        "store_id",
        "activity_month",
        "min_sqft",
        "distinct_store_ids",
        "days_since_order_date_avg_3_p90",
        F.col("sum_inventory_quantity"),
    )
    .orderBy(F.col("activity_month").asc_nulls_first())
)
