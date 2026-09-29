from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("carrier").orderBy("order_date", "id")
w2 = Window.orderBy("delivered_date", "id")
w3 = Window.partitionBy("order_id").orderBy("cost", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("order_id").orderBy(F.col("order_date").desc(), "id")

base = (
    df_shipments
    .filter(F.col("delivered_date") < "2024-07-13")
    .select(
        "id",
        "order_id",
        "carrier",
        "shipped_date",
        "delivered_date",
        "cost",
        F.when(F.col("status").isin(["DELIVERED", "LOST"]), "Tier 1").otherwise("Other").alias("status_group"),
        F.coalesce("carrier", F.lit("DIRECT")).alias("carrier_clean"),
    )
)

joined = (
    base
    .alias("s")
    .join(df_orders.alias("b"), F.col("s.order_id").eqNullSafe(F.col("b.id")), "left")
    .select("s.*", F.col("b.amount"), F.col("b.order_date"), F.col("b.shipping_fee"))
)

enriched = (
    joined
    .select(
        "id",
        "order_id",
        "shipped_date",
        "delivered_date",
        "cost",
        "status_group",
        "amount",
        "order_date",
        F.row_number().over(w).alias("seq_no"),
        F.row_number().over(w2).alias("row_seq"),
        F.sum("shipping_fee").over(w3).alias("running_shipping_fee"),
        F.row_number().over(w4).alias("row_seq_2"),
    )
)

valid = (
    enriched
    .filter(F.col("shipped_date") > F.lit("2023-08-04").cast("date"))
    .select(
        "id",
        "order_id",
        "shipped_date",
        "delivered_date",
        "cost",
        "status_group",
        "amount",
        "order_date",
        "seq_no",
        "row_seq",
        "running_shipping_fee",
        "row_seq_2",
        F.when(F.col("row_seq_2") >= 7, "High").when(F.col("row_seq_2") >= 4, "Medium").otherwise("Low").alias("row_seq_2_band"),
    )
)

result = (
    valid
    .groupBy(
        F.date_trunc("week", "shipped_date").alias("week_start"),
        F.date_trunc("year", "shipped_date").alias("year_start"),
    )
    .agg(
        F.percentile_approx("amount", [0.25, 0.5, 0.75]).alias("amount_quartiles"),
        F.percentile_approx("row_seq_2", [0.25, 0.5, 0.75]).alias("row_seq_2_quartiles"),
        F.percentile_approx("seq_no", 0.95).alias("seq_no_p95"),
    )
    .orderBy(F.col("seq_no_p95").desc())
)
