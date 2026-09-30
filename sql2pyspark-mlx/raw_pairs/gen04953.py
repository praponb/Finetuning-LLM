from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.year("order_date"), "city").orderBy(F.col("discount").desc())
discount_by_customer_id = df_orders.groupBy("customer_id").agg(F.avg("discount").alias("avg_order_discount"))

kept_keys = (
    df_orders
    .select("status")
    .exceptAll(
        df_orders
        .filter(F.col("shipping_fee") > 14)
        .select("status")
    )
)

reconciled = (
    df_orders
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.status") == F.col("k.status")), "inner")
    .select("s.*")
)

kept = (
    reconciled
    .alias("s")
    .join(df_payments.alias("c"), (F.col("c.order_id") == F.col("s.id")) & (F.col("c.paid_at") < F.col("s.order_date")), "left_anti")
)

enriched = (
    kept
    .alias("s")
    .join(df_stores.alias("p"), F.col("s.store_id") == F.col("p.id"), "inner")
    .select(
        "s.customer_id",
        "s.store_id",
        "s.order_date",
        "s.status",
        "s.amount",
        "s.discount",
        F.col("p.city"),
        F.col("p.sqft"),
    )
)

enriched_orders = (
    enriched
    .alias("e")
    .join(df_stores.alias("p"), F.col("e.store_id") == F.col("p.id"), "left")
    .select("e.*", F.col("p.region"), F.col("p.store_name"))
)

scored = (
    enriched_orders
    .filter(F.col("city").isin(["London", "Tokyo"]))
    .withColumn("discount_cume_dist", F.cume_dist().over(w))
)

result = (
    scored
    .join(discount_by_customer_id, on="customer_id", how="left")
    .select(
        "customer_id",
        "store_id",
        "order_date",
        "discount",
        "city",
        "sqft",
        "region",
        "discount_cume_dist",
        F.col("avg_order_discount"),
    )
    .orderBy(F.col("region").desc_nulls_last())
)
