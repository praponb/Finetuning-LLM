from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("shipped_date").desc(), "id")
w_ordered = Window.orderBy("shipping_fee", "id")

filtered = (
    df_shipments
    .filter((F.col("carrier").eqNullSafe(F.lit("DHL"))) & F.col("order_id").isNull())
    .withColumn("clean_carrier", F.trim("carrier"))
    .withColumn("carrier_filled", F.coalesce("carrier", F.lit("NONE")))
)

with_dims = (
    filtered
    .alias("a")
    .join(df_orders.alias("o"), F.col("a.order_id") == F.col("o.id"), "inner")
    .select("a.*", F.col("o.amount"))
)

enriched = (
    with_dims
    .alias("e")
    .join(df_orders.alias("o"), F.col("e.order_id") == F.col("o.id"), "inner")
    .select(
        "e.id",
        "e.order_id",
        "e.shipped_date",
        "e.cost",
        "e.status",
        F.col("o.shipping_fee"),
    )
)

ranked = (
    enriched
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
    .select("id", "shipped_date", "cost", "status", "shipping_fee")
)

cleaned = (
    ranked
    .filter(F.col("shipped_date") > F.lit("2023-09-04").cast("date"))
    .select(
        "id",
        "cost",
        "shipping_fee",
        F.initcap(F.trim("status")).alias("status_title"),
    )
)

cleaned_shipments = (
    cleaned
    .filter(F.col("cost") < 68)
    .withColumn("least_cost_shipping_fee", F.least("cost", "shipping_fee"))
)

scored = (
    cleaned_shipments
    .select(
        "id",
        "shipping_fee",
        F.lag("shipping_fee").over(w_ordered).alias("prev_shipping_fee"),
        F.lag("cost").over(w_ordered).alias("prev_cost"),
        F.lead("cost").over(w_ordered).alias("next_cost"),
        F.row_number().over(w_ordered).alias("seq_no"),
    )
)

result = (
    scored
    .withColumn("next_cost_tier", F.when(F.col("next_cost") >= 87, "PREMIUM").otherwise("STANDARD"))
    .groupingSets(
        [["next_cost_tier", "seq_no"], ["next_cost_tier"]],
        "next_cost_tier",
        "seq_no",
    )
    .agg(
        F.round(F.avg("seq_no"), 2).alias("avg_seq_no"),
        F.countDistinct("id").alias("distinct_ids"),
        F.percentile_approx("prev_shipping_fee", [0.25, 0.5, 0.75]).alias("prev_shipping_fee_quartiles"),
    )
    .filter(F.col("distinct_ids") > 1)
    .orderBy(F.col("next_cost_tier").desc_nulls_last())
)
