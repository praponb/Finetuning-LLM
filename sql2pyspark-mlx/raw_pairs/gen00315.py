from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.month("delivered_date"), "carrier").orderBy("months_delivered_date_to_shipped_date")
w2 = Window.partitionBy("carrier", "status")
w3 = Window.partitionBy("status", "carrier")
w4 = Window.partitionBy("status").orderBy(F.col("status_min_months_delivered_date_to_shipped_date").desc_nulls_first())
w5 = Window.orderBy(F.col("months_delivered_date_to_shipped_date").asc_nulls_last())
w6 = Window.orderBy("status_min_months_delivered_date_to_shipped_date_cume_dist")
w7 = Window.orderBy(F.col("months_delivered_date_to_shipped_date").desc())
w8 = Window.orderBy(F.col("cost").asc_nulls_last())

base = (
    df_shipments
    .filter((F.col("shipped_date") >= F.lit("2023-05-05").cast("date")) & F.col("order_id").isNotNull())
    .select(
        "id",
        "order_id",
        "carrier",
        "delivered_date",
        "cost",
        "status",
        F.round((F.datediff("shipped_date", "delivered_date") / 30), 1).alias("months_delivered_date_to_shipped_date"),
    )
)

kept_keys = (
    base
    .select("id")
    .exceptAll(
        df_shipments
        .filter(F.col("shipped_date") <= "2024-03-01")
        .select("id")
    )
)

verified = (
    base
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

scored = (
    verified
    .select(
        "id",
        "carrier",
        "delivered_date",
        "cost",
        "status",
        "months_delivered_date_to_shipped_date",
        F.cume_dist().over(w).alias("months_delivered_date_to_shipped_date_cume_dist"),
        F.count("months_delivered_date_to_shipped_date").over(w2).alias("carrier_count_months_delivered_date_to_shipped_date"),
        F.min("months_delivered_date_to_shipped_date").over(w3).alias("status_min_months_delivered_date_to_shipped_date"),
    )
)

enriched = (
    scored
    .filter(F.col("status_min_months_delivered_date_to_shipped_date") >= 37)
    .select(
        "id",
        "cost",
        "months_delivered_date_to_shipped_date",
        F.cume_dist().over(w4).alias("status_min_months_delivered_date_to_shipped_date_cume_dist"),
    )
)

base_shipments = (
    enriched
    .filter(F.col("months_delivered_date_to_shipped_date") < 53)
    .select(
        "*",
        (F.col("status_min_months_delivered_date_to_shipped_date_cume_dist") - F.col("cost")).alias("status_min_months_delivered_date_to_shipped_date_cume_dist_minus_cost"),
    )
)

scored_shipments = (
    base_shipments
    .select(
        "*",
        F.cume_dist().over(w5).alias("months_delivered_date_to_shipped_date_cume_dist"),
        F.cume_dist().over(w6).alias("status_min_months_delivered_date_to_shipped_date_cume_dist_cume_dist"),
    )
)

metrics = (
    scored_shipments
    .select(
        "id",
        "months_delivered_date_to_shipped_date",
        "status_min_months_delivered_date_to_shipped_date_cume_dist",
        "status_min_months_delivered_date_to_shipped_date_cume_dist_minus_cost",
        F.cume_dist().over(w7).alias("months_delivered_date_to_shipped_date_cume_dist_2"),
        F.cume_dist().over(w8).alias("cost_cume_dist"),
    )
)

result = (
    metrics
    .select(
        "months_delivered_date_to_shipped_date",
        "status_min_months_delivered_date_to_shipped_date_cume_dist",
        "months_delivered_date_to_shipped_date_cume_dist_2",
        "cost_cume_dist",
    )
    .orderBy(
        F.col("status_min_months_delivered_date_to_shipped_date_cume_dist").desc_nulls_last(),
        "months_delivered_date_to_shipped_date",
    )
)
