from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy(F.col("adjusted_cost").desc_nulls_last(), "id")
w2 = Window.partitionBy("order_id").orderBy("delivered_date", "id")
w3 = Window.partitionBy("channel")

valid = (
    df_shipments
    .filter(F.col("carrier").isNotNull() & (F.col("order_id") == 40))
    .select(
        "id",
        "order_id",
        "shipped_date",
        "delivered_date",
        "cost",
        F.round((F.col("cost") * 1.15), 2).alias("adjusted_cost"),
    )
)

ranked = (
    valid
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

enriched = (
    ranked
    .select(
        "*",
        F.lead("adjusted_cost").over(w2).alias("next_adjusted_cost"),
        F.lead("cost").over(w2).alias("next_cost"),
        F.lag("cost").over(w2).alias("prev_cost"),
        F.row_number().over(w2).alias("seq_no"),
    )
)

enriched_shipments = (
    enriched
    .alias("src")
    .join(df_orders.alias("o"), F.col("src.order_id") == F.col("o.id"), "left")
    .select("src.*", F.col("o.channel"), F.col("o.amount"))
)

matched_keys = (
    enriched_shipments
    .select("order_id", "id")
    .exceptAll(
        df_shipments
        .filter(F.col("carrier") == "FedEx")
        .select("order_id", "id")
    )
)

kept_rows = (
    enriched_shipments
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.order_id") == F.col("k.order_id")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

kept_keys = (
    kept_rows
    .select("order_id", "id")
    .subtract(
        df_shipments
        .filter(F.col("carrier").isNull())
        .select("order_id", "id")
    )
)

kept_rows_shipments = (
    kept_rows
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.order_id") == F.col("k.order_id")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

enriched_shipments_2 = (
    kept_rows_shipments
    .select(
        "id",
        "shipped_date",
        "adjusted_cost",
        "next_adjusted_cost",
        "prev_cost",
        "seq_no",
        "channel",
        F.round((F.col("cost") / F.sum("cost").over(w3)), 4).alias("cost_share"),
    )
)

result = (
    enriched_shipments_2
    .cube("channel", F.date_trunc("YEAR", "shipped_date").alias("year_start"), "seq_no")
    .agg(
        F.count(F.when(F.col("channel") == "mobile", 1)).alias("mobile_count"),
        F.sum(F.when(F.col("channel") == "web", F.col("prev_cost")).otherwise(0)).alias("web_prev_cost"),
    )
    .orderBy(F.col("channel").desc_nulls_first())
)
