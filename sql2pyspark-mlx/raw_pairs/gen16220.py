from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("shipped_date", "id")
w2 = Window.partitionBy("status").orderBy("shipped_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("order_status", "order_id").orderBy("amount", "id")

joined = (
    df_shipments
    .alias("s")
    .join(df_orders.alias("b"), F.col("s.order_id") == F.col("b.id"), "left")
    .select("s.*", F.col("b.channel"))
)

scored = (
    joined
    .withColumn("prev_cost", F.lag("cost").over(w))
    .withColumn("next_cost", F.lead("cost").over(w))
    .withColumn("seq_no", F.row_number().over(w))
    .withColumn("last_known_cost", F.last("cost", ignorenulls=True).over(w2))
)

base = (
    scored
    .filter(F.col("delivered_date") > F.to_date(F.lit("2023-10-04")))
    .select(
        "*",
        F.coalesce("cost", F.lit(0)).alias("cost_filled"),
        F.add_months("shipped_date", 12).alias("shipped_date_plus_months"),
    )
)

with_dims = (
    base
    .alias("src")
    .join(df_orders.alias("d"), F.col("src.order_id") == F.col("d.id"), "left")
    .select(
        "src.id",
        "src.order_id",
        "src.carrier",
        "src.status",
        "src.next_cost",
        "src.last_known_cost",
        "src.cost_filled",
        "src.shipped_date_plus_months",
        F.col("d.amount"),
        F.col("d.order_date"),
        F.col("d.status").alias("order_status"),
    )
)

deduped = (
    with_dims
    .withColumn("row_num", F.row_number().over(w3))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

cleaned = (
    deduped
    .filter(F.col("order_id").isNotNull())
    .withColumn("next_cost_band", F.when(F.col("next_cost") >= 93, "High").when(F.col("next_cost") >= 48, "Medium").otherwise("Low"))
    .withColumn("weeks_shipped_date_plus_months_to_order_date", (F.datediff("order_date", "shipped_date_plus_months") / 7))
)

enriched = (
    cleaned
    .alias("src")
    .join(df_orders.alias("o"), F.col("src.order_id").eqNullSafe(F.col("o.id")), "left")
    .select(
        "src.*",
        F.col("o.status").alias("order_status_2"),
        F.col("o.order_date").alias("order_order_date"),
    )
)

result = (
    enriched
    .select(
        "id",
        "order_id",
        "carrier",
        "status",
        "next_cost",
        "last_known_cost",
        "cost_filled",
        "amount",
        "order_date",
        F.when(F.col("carrier").rlike("^(Kerry|FedEx)$"), "GROUP_B").when(F.col("carrier").rlike("^(DHL|Flash)$"), "EXTENDED").otherwise("OTHER_ZONE").alias("carrier_zone"),
    )
    .orderBy(F.col("order_id").desc())
)
