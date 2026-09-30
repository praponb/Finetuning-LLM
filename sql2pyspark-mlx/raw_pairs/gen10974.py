from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("status_length", "id")
w2 = Window.partitionBy("status")
w3 = Window.partitionBy("status", "order_id").orderBy("shipping_fee", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("order_id")
w_recent = Window.partitionBy("order_id", "status")
w_grp = Window.partitionBy("order_id").orderBy("shipped_date", "id")
w7 = Window.partitionBy(F.year("shipped_date"), "status")
w8 = Window.partitionBy("order_id", "status").orderBy(F.col("status_count_amount").desc(), "id")

base = (
    df_shipments
    .filter(F.col("carrier").isNotNull() & (~F.col("carrier").eqNullSafe(F.lit("UPS"))))
    .select("*", F.length("status").alias("status_length"))
)

enriched = (
    base
    .alias("e")
    .join(df_orders.alias("d"), F.col("e.order_id").eqNullSafe(F.col("d.id")), "inner")
    .select(
        "e.id",
        "e.order_id",
        "e.shipped_date",
        "e.status",
        "e.status_length",
        F.col("d.amount"),
        F.col("d.shipping_fee"),
    )
)

scored = (
    enriched
    .filter(F.col("amount") <= 480)
    .select(
        "*",
        F.lag("status_length", 2, 0).over(w).alias("prev_status_length"),
        F.count("amount").over(w2).alias("status_count_amount"),
        F.sum("status_length").over(w3).alias("running_status_length"),
    )
)

enriched_shipments = (
    scored
    .select(
        "id",
        "status",
        "shipping_fee",
        "status_count_amount",
        F.sum("running_status_length").over(w4).alias("order_id_sum_running_status_length"),
        F.stddev_samp("running_status_length").over(w_recent).alias("order_id_stddev_samp_running_status_length"),
        F.row_number().over(w_grp).alias("seq_no"),
        F.round((F.col("status_count_amount") / F.sum("status_count_amount").over(w7)), 4).alias("status_count_amount_share"),
        F.ntile(4).over(w8).alias("status_count_amount_bucket"),
    )
)

result = (
    enriched_shipments
    .select(
        "id",
        "status",
        "status_count_amount_share",
        F.col("shipping_fee").alias("SHIPPING_FEE_VAL"),
    )
    .unpivot(
        ids=["id", "status"],
        values=["status_count_amount_share", "SHIPPING_FEE_VAL"],
        variableColumnName="kpi",
        valueColumnName="val",
    )
    .filter(F.col("val").isNotNull())
)
