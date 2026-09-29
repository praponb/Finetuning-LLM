from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id", "status")
w2 = Window.partitionBy("order_id").orderBy(F.col("order_id_stddev_samp_amount").desc_nulls_last())

matched_keys = (
    df_shipments
    .select("order_id", "id")
    .subtract(
        df_shipments
        .filter(F.year("delivered_date") == 2025)
        .select("order_id", "id")
    )
)

reconciled = (
    df_shipments
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.order_id") == F.col("k.order_id")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

enriched = (
    reconciled
    .alias("s")
    .join(df_orders.alias("p"), F.col("s.order_id") == F.col("p.id"), "inner")
    .select("s.*", F.col("p.discount"), F.col("p.amount"), F.col("p.order_date"))
)

scored = (
    enriched
    .select(
        "id",
        "order_id",
        "discount",
        "order_date",
        F.stddev_samp("amount").over(w).alias("order_id_stddev_samp_amount"),
    )
)

metrics = (
    scored
    .select(
        "order_id",
        "discount",
        "order_id_stddev_samp_amount",
        F.rank().over(w2).alias("order_id_stddev_samp_amount_rank"),
    )
)

valid = (
    metrics
    .filter(F.col("discount") <= 0.24)
    .withColumn("order_id_stddev_samp_amount_times_discount", (F.col("order_id_stddev_samp_amount") * F.col("discount")))
)

result = (
    valid
    .select(
        "order_id",
        "discount",
        F.col("order_id_stddev_samp_amount_times_discount").alias("ORDER_ID_STDDEV_SAMP_AMOUNT_TIMES_DISCOUNT_VAL"),
    )
    .unpivot(
        ids=["order_id"],
        values=["discount", "ORDER_ID_STDDEV_SAMP_AMOUNT_TIMES_DISCOUNT_VAL"],
        variableColumnName="kpi",
        valueColumnName="reading",
    )
)
