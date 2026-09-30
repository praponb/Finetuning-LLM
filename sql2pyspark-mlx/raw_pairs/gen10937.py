from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.quantity").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("customer_id")
w3 = Window.partitionBy("customer_id", "status")

with_latest = (
    df_orders
    .alias("s")
    .join(df_order_items.alias("c"), (F.col("c.order_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.customer_id",
        "s.order_date",
        "s.status",
        "s.amount",
        F.col("c.quantity"),
        F.col("c.unit_price").alias("order_item_unit_price"),
    )
)

kept_keys = (
    with_latest
    .select("customer_id")
    .exceptAll(
        df_orders
        .filter(F.col("status") != "RETURNED")
        .select("customer_id")
    )
)

kept_rows = (
    with_latest
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.customer_id") == F.col("k.customer_id")), "inner")
    .select("s.*")
)

cleaned = (
    kept_rows
    .filter(F.col("customer_id").isNotNull())
    .select(
        "*",
        (F.col("amount") * F.col("order_item_unit_price")).alias("amount_times_order_item_unit_price"),
    )
)

not_flagged = (
    cleaned
    .alias("s")
    .join(df_payments.alias("x"), (F.col("x.order_id") == F.col("s.id")) & (F.col("x.paid_at") <= F.col("s.order_date")), "left_anti")
)

scored = (
    not_flagged
    .select(
        "*",
        F.stddev_samp("amount_times_order_item_unit_price").over(w2).alias("customer_id_stddev_samp_amount_times_order_item_unit_price"),
        F.round((F.col("order_item_unit_price") / F.sum("order_item_unit_price").over(w3)), 4).alias("order_item_unit_price_share"),
    )
)

result = (
    scored
    .select(
        "customer_id",
        F.col("order_item_unit_price").alias("ORDER_ITEM_UNIT_PRICE_VAL"),
        F.col("amount").alias("AMOUNT"),
        F.col("amount_times_order_item_unit_price").alias("AMOUNT_TIMES_ORDER_ITEM_UNIT_PRICE"),
    )
    .unpivot(
        ids=["customer_id"],
        values=["ORDER_ITEM_UNIT_PRICE_VAL", "AMOUNT", "AMOUNT_TIMES_ORDER_ITEM_UNIT_PRICE"],
        variableColumnName="metric_name",
        valueColumnName="val",
    )
    .filter(F.col("val").isNotNull())
)
