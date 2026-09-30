from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id")
w_part = Window.partitionBy("channel", "customer_id").orderBy(F.col("discount").desc_nulls_first())

remaining_keys = (
    df_orders
    .select("customer_id")
    .exceptAll(
        df_orders
        .filter(F.col("customer_id").isNotNull())
        .select("customer_id")
    )
)

kept_rows = (
    df_orders
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.customer_id") == F.col("k.customer_id")), "inner")
    .select("s.*")
)

base = (
    kept_rows
    .filter(~F.col("channel").eqNullSafe(F.lit("mobile")))
    .select(
        "id",
        "customer_id",
        "store_id",
        "order_date",
        "status",
        "channel",
        "amount",
        "discount",
        F.coalesce("channel", F.lit("unassigned")).alias("channel_filled"),
        F.coalesce("channel", F.lit("DIRECT")).alias("channel_filled_2"),
    )
)

scored = (
    base
    .select(
        "customer_id",
        "order_date",
        "status",
        "channel",
        "discount",
        "channel_filled_2",
        F.corr("discount", "amount").over(w).alias("corr_discount_amount"),
        F.dense_rank().over(w_part).alias("discount_dense_rank"),
    )
)

base_orders = (
    scored
    .filter(F.col("customer_id").isNotNull())
    .select(
        "customer_id",
        "order_date",
        "status",
        "discount",
        "channel_filled_2",
        "corr_discount_amount",
        "discount_dense_rank",
        (F.col("corr_discount_amount") + F.col("discount")).alias("corr_discount_amount_plus_discount"),
        F.coalesce("channel", F.lit("OTHER")).alias("resolved_channel"),
    )
)

result = (
    base_orders
    .select(
        "customer_id",
        "corr_discount_amount_plus_discount",
        F.col("discount").alias("DISCOUNT_VAL"),
        F.col("corr_discount_amount").alias("CORR_DISCOUNT_AMOUNT_VAL"),
    )
    .unpivot(
        ids=["customer_id"],
        values=["corr_discount_amount_plus_discount", "DISCOUNT_VAL", "CORR_DISCOUNT_AMOUNT_VAL"],
        variableColumnName="metric_name",
        valueColumnName="metric_value",
    )
    .filter(F.col("metric_value").isNotNull())
)
