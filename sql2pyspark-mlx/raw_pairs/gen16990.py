from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("delivered_date", "id", "promo_id")
w2 = Window.partitionBy("order_id")
w3 = Window.partitionBy("order_id").orderBy("months_since_shipped_date", "id", "promo_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.orderBy("amount", "id", "promo_id")
cost_by_order_id = df_shipments.groupBy("order_id").agg(F.min("cost").alias("min_shipment_cost"))

with_dims = (
    df_shipments
    .alias("s")
    .join(df_orders.alias("p"), F.col("s.order_id").eqNullSafe(F.col("p.id")), "left")
    .select("s.*", F.col("p.shipping_fee"), F.col("p.channel"), F.col("p.discount"))
)

valid = (
    with_dims
    .filter(F.col("channel").isin(["web", "partner", "mobile"]))
    .select(
        "*",
        (F.datediff(F.current_date(), "shipped_date") / 30).alias("months_since_shipped_date"),
    )
)

base = (
    valid
    .filter(F.col("channel") == "store")
    .select(
        "id",
        "order_id",
        "delivered_date",
        "cost",
        "shipping_fee",
        "discount",
        "months_since_shipped_date",
        F.initcap(F.trim("carrier")).alias("carrier_title"),
        (F.col("cost") / F.nullif("discount", F.lit(0))).alias("cost_per_discount"),
    )
)

with_dims_shipments = (
    base
    .alias("a")
    .join(df_orders.alias("p"), F.col("a.order_id") == F.col("p.id"), "left")
    .filter(F.col("p.amount") > 590)
    .select(
        "a.id",
        "a.order_id",
        "a.delivered_date",
        "a.cost",
        "a.discount",
        "a.months_since_shipped_date",
        "a.carrier_title",
        "a.cost_per_discount",
        F.col("p.amount"),
        F.col("p.status"),
        F.col("p.channel"),
    )
)

with_window = (
    with_dims_shipments
    .alias("e")
    .join(df_promotions.alias("pr"), (F.col("e.channel") == F.col("pr.channel")) & (F.col("e.delivered_date") >= F.col("pr.starts_on")) & (F.col("e.delivered_date") < F.col("pr.ends_on") + F.expr("INTERVAL 14 DAYS")), "left")
    .select(
        "e.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code"),
        F.col("pr.discount_pct"),
    )
)

scored = (
    with_window
    .select(
        "id",
        "order_id",
        "cost",
        "discount",
        "months_since_shipped_date",
        "cost_per_discount",
        "amount",
        "status",
        "channel",
        "promo_id",
        "discount_pct",
        F.lead("cost", 1).over(w).alias("next_cost"),
        F.round((F.col("discount_pct") / F.sum("discount_pct").over(w2)), 4).alias("discount_pct_share"),
    )
)

scored_shipments = (
    scored
    .select(
        "id",
        "order_id",
        "cost",
        "months_since_shipped_date",
        "status",
        "channel",
        "promo_id",
        "discount_pct",
        "discount_pct_share",
        F.sum("cost").over(w3).alias("running_cost"),
        F.round((F.col("next_cost") / F.sum("next_cost").over(w2)), 4).alias("next_cost_share"),
        F.lag("promo_id", 1).over(w4).alias("prev_promo_id"),
    )
)

result = (
    scored_shipments
    .join(cost_by_order_id, on="order_id", how="left")
    .select(
        "id",
        "order_id",
        "cost",
        "status",
        "promo_id",
        "discount_pct_share",
        "running_cost",
        "next_cost_share",
        F.when(F.col("status").rlike("^(RETURNED|SHIPPED)$"), "PRIORITY").when(F.col("status").rlike("^(COMPLETED|PENDING)$"), "EXTENDED").otherwise("CORE").alias("status_zone"),
        F.col("min_shipment_cost"),
    )
    .orderBy(F.col("min_shipment_cost").asc_nulls_first())
)
