from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.month("order_date"), "status")
w2 = Window.partitionBy("store_id").orderBy(F.col("discount").asc_nulls_first())
w3 = Window.partitionBy(F.year("order_date"), "status").orderBy(F.col("amount").desc())
amount_by_customer_id = df_orders.groupBy("customer_id").agg(F.max("amount").alias("max_order_amount"))

eligible = (
    df_orders
    .alias("s")
    .join(df_order_items.alias("c"), (F.col("c.order_id") == F.col("s.id")) & (F.col("c.quantity") == 4), "left_anti")
)

remaining_keys = (
    eligible
    .select("customer_id", "status")
    .subtract(
        df_orders
        .filter(F.col("customer_id").isin([17, 25, 33, 34]))
        .select("customer_id", "status")
    )
)

kept_rows = (
    eligible
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.customer_id") == F.col("k.customer_id")) & (F.col("s.status") == F.col("k.status")), "inner")
    .select("s.*")
)

kept = (
    kept_rows
    .alias("s")
    .join(df_shipments.alias("x"), (F.col("x.order_id") == F.col("s.id")) & (F.month(F.col("x.delivered_date")).isin([2, 8, 10])), "left_anti")
)

cleaned = (
    kept
    .filter((~F.col("channel").eqNullSafe(F.lit("store"))) & (F.col("order_date") >= F.lit("2023-04-11").cast("date")))
    .select("*", F.col("discount").cast("int").alias("discount_int"))
)

with_windows = (
    cleaned
    .filter(F.col("discount") < 0.3)
    .select(
        "customer_id",
        "store_id",
        "order_date",
        "amount",
        "shipping_fee",
        "discount_int",
        F.corr("shipping_fee", "discount").over(w).alias("corr_shipping_fee_discount"),
        F.dense_rank().over(w2).alias("discount_dense_rank"),
        F.percent_rank().over(w3).alias("amount_percent_rank"),
    )
)

base = (
    with_windows
    .filter((F.col("order_date") >= F.to_date(F.lit("2023-08-19"))) & F.col("customer_id").isin([9, 21, 22, 33]))
    .select(
        "*",
        F.abs("corr_shipping_fee_discount").alias("abs_corr_shipping_fee_discount"),
        F.col("shipping_fee").cast("int").alias("shipping_fee_int"),
    )
)

enriched = (
    base
    .alias("src")
    .join(df_customers.alias("c"), F.col("src.customer_id") == F.col("c.id"), "left")
    .select(
        "src.customer_id",
        "src.store_id",
        "src.order_date",
        "src.discount_int",
        "src.amount_percent_rank",
        "src.abs_corr_shipping_fee_discount",
        F.col("c.segment"),
        F.col("c.credit_limit"),
    )
)

stats = (
    enriched
    .groupBy("customer_id")
    .agg(
        F.percentile_approx("discount_int", 0.5).alias("discount_int_p50"),
        F.sum(F.when(F.col("segment") == "SMB", F.col("credit_limit")).otherwise(0)).alias("smb_credit_limit"),
        F.sum(F.when(F.col("segment") == "Consumer", F.col("amount_percent_rank")).otherwise(0)).alias("consumer_amount_percent_rank"),
        F.count_if(F.col("segment") == "SMB").alias("smb_events"),
        F.sum(F.when(F.col("segment") == "Corporate", F.col("discount_int")).otherwise(0)).alias("corporate_discount_int"),
    )
)

result = (
    stats
    .join(amount_by_customer_id, on="customer_id", how="left")
    .select(
        "customer_id",
        "discount_int_p50",
        "smb_events",
        "corporate_discount_int",
        F.col("max_order_amount"),
    )
    .orderBy("smb_events")
)
