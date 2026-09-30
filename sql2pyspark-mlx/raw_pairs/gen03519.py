from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("carrier", "order_id").orderBy(F.col("cost").desc(), "id")
w2 = Window.orderBy("delivered_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("status", "order_id").orderBy(F.col("cost").asc_nulls_first())
w4 = Window.partitionBy("carrier").orderBy(F.col("cost").desc(), "id")

scored = (
    df_shipments
    .filter(F.col("order_id") == 26)
    .select(
        "*",
        F.ntile(4).over(w).alias("cost_bucket"),
        F.first("carrier", ignorenulls=True).over(w2).alias("first_known_carrier"),
        F.rank().over(w3).alias("cost_rank"),
    )
)

ranked = (
    scored
    .withColumn("rn", F.row_number().over(w4))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

valid = (
    ranked
    .filter(F.col("order_id").isNotNull())
    .select(
        "id",
        "order_id",
        "carrier",
        "status",
        "cost_bucket",
        "first_known_carrier",
        "cost_rank",
        F.add_months("delivered_date", 6).alias("delivered_date_plus_months"),
    )
)

joined = (
    valid
    .alias("src")
    .join(df_orders.alias("d"), F.col("src.order_id") == F.col("d.id"), "left")
    .select(
        "src.*",
        F.col("d.channel"),
        F.col("d.amount"),
        F.col("d.status").alias("order_status"),
    )
)

matched = (
    joined
    .alias("a")
    .join(df_promotions.alias("pr"), (F.col("a.channel") == F.col("pr.channel")) & F.col("a.delivered_date_plus_months").between(F.col("pr.starts_on"), F.col("pr.ends_on")), "inner")
    .select(
        "a.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code"),
        F.col("pr.discount_pct"),
    )
)

totals = (
    matched
    .groupBy(
        F.when(F.col("cost_rank") >= 8, "PREMIUM").otherwise("STANDARD").alias("cost_rank_tier"),
    )
    .agg(
        F.sort_array(F.collect_set("status")).alias("status_list"),
        F.round(F.avg("cost_rank"), 2).alias("avg_cost_rank"),
    )
    .filter(F.col("avg_cost_rank") >= 0)
)

result = (
    totals
    .select("cost_rank_tier", "status_list")
    .orderBy(F.col("cost_rank_tier").desc())
)
