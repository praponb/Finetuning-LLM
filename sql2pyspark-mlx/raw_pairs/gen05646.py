from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("discount").desc_nulls_first(), "id")

base = (
    df_orders
    .filter(F.col("channel").isNotNull() & (F.col("amount") < 1190))
    .select(
        "*",
        F.add_months("order_date", 12).alias("order_date_plus_months"),
        F.coalesce("channel", F.lit("unassigned")).alias("channel_filled"),
        F.coalesce("channel", F.lit("UNKNOWN")).alias("resolved_channel"),
    )
)

deduped = (
    base
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
    .select(
        "id",
        "customer_id",
        "order_date",
        "status",
        "amount",
        "shipping_fee",
        "channel_filled",
    )
)

not_flagged = (
    deduped
    .alias("s")
    .join(df_shipments.alias("c"), (F.col("c.order_id") == F.col("s.id")) & (F.col("c.carrier").isNull()), "left_semi")
)

remaining_keys = (
    not_flagged
    .select("customer_id", "status")
    .intersect(
        df_orders
        .filter(F.col("amount") > 1560)
        .select("customer_id", "status")
    )
)

kept_rows = (
    not_flagged
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.customer_id") == F.col("k.customer_id")) & (F.col("s.status") == F.col("k.status")), "inner")
    .select("s.*")
)

result = kept_rows.select("customer_id", "shipping_fee", "channel_filled")
