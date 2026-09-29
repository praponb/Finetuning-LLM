from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("carrier").orderBy(F.col("shipping_fee").asc_nulls_last())
w_grp = Window.partitionBy("status", "order_status_2").orderBy("latest_rank", "id")
quantity_by_order_id = df_order_items.groupBy("order_id").agg(F.min("quantity").alias("min_order_item_quantity"))

with_dims = (
    df_shipments
    .alias("a")
    .join(df_orders.alias("d"), F.col("a.order_id").eqNullSafe(F.col("d.id")), "left")
    .filter(F.col("d.discount") < 0.34)
    .select(
        "a.*",
        F.col("d.status").alias("order_status"),
        F.col("d.shipping_fee"),
        F.col("d.amount"),
    )
)

deduped = (
    with_dims
    .withColumn("latest_rank", F.rank().over(w))
    .filter(F.col("latest_rank") == 1)
)

joined = (
    deduped
    .alias("s")
    .join(df_orders.alias("d"), F.col("s.order_id").eqNullSafe(F.col("d.id")), "inner")
    .select("s.*", F.col("d.order_date"), F.col("d.status").alias("order_status_2"))
)

with_windows = (
    joined
    .select(
        "*",
        F.lead("amount").over(w_grp).alias("next_amount"),
        F.lag("amount").over(w_grp).alias("prev_amount"),
        F.lead("shipping_fee").over(w_grp).alias("next_shipping_fee"),
        F.lag("shipping_fee").over(w_grp).alias("prev_shipping_fee"),
        F.row_number().over(w_grp).alias("seq_no"),
    )
)

with_dims_shipments = (
    with_windows
    .alias("e")
    .join(df_orders.alias("d"), F.col("e.order_id") == F.col("d.id"), "left")
    .select(
        "e.*",
        F.col("d.status").alias("order_status_3"),
        F.col("d.shipping_fee").alias("order_shipping_fee"),
    )
)

kept_keys = (
    with_dims_shipments
    .select("status", "id")
    .subtract(
        df_shipments
        .filter(F.col("shipped_date").between("2023-03-01", "2025-05-01"))
        .select("status", "id")
    )
)

kept_rows = (
    with_dims_shipments
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.status") == F.col("k.status")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .join(quantity_by_order_id, on="order_id", how="left")
    .select(
        "order_id",
        "carrier",
        "shipped_date",
        "delivered_date",
        "cost",
        "order_status",
        "latest_rank",
        "order_date",
        "order_status_2",
        F.when(F.col("order_status").rlike("^(CANCELLED|RETURNED)$"), "OTHER_ZONE").otherwise("EXTENDED").alias("order_status_zone"),
        F.col("min_order_item_quantity"),
    )
    .orderBy(F.col("carrier").desc_nulls_first(), F.col("shipped_date").desc())
)
