from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.partitionBy("status", "carrier")
w2 = Window.partitionBy("status")
w3 = Window.partitionBy("status").orderBy("cost_int")
w_hist = Window.partitionBy("status").orderBy(F.col("cost_int").asc_nulls_first())
w5 = Window.partitionBy("status").orderBy(F.col("cost_int_rank").desc_nulls_first())
w6 = Window.orderBy(F.col("cost_int_percent_rank").desc_nulls_first())
w7 = Window.partitionBy("carrier").orderBy(F.col("cost").desc())
w8 = Window.partitionBy("carrier").orderBy(F.col("cost_int").desc_nulls_first())
w9 = Window.orderBy(F.col("cost_rank").desc_nulls_last())
w10 = Window.partitionBy(F.year("shipped_date"))
w11 = Window.orderBy(F.col("cost_int").desc_nulls_last())

valid = (
    df_shipments
    .filter(F.col("carrier").isNotNull() & (~F.col("carrier").eqNullSafe(F.lit("UPS"))))
    .select(
        "id",
        "carrier",
        "shipped_date",
        "cost",
        "status",
        F.concat("status", F.lit("/"), "carrier").alias("status_carrier"),
        F.trim("status").alias("clean_status"),
        F.coalesce("carrier", F.lit("NONE")).alias("carrier_filled"),
    )
)

filtered = (
    valid
    .filter(F.year("shipped_date") >= 2025)
    .select(
        "id",
        "carrier",
        "shipped_date",
        "cost",
        "status",
        "status_carrier",
        "clean_status",
        "carrier_filled",
        F.initcap(F.trim("status")).alias("status_title"),
    )
)

matched_keys = (
    filtered
    .select("id", "status")
    .intersectAll(
        df_shipments
        .filter(F.col("order_id") == 30)
        .select("id", "status")
    )
)

verified = (
    filtered
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.status") == F.col("k.status")), "inner")
    .select("s.*")
)

valid_shipments = (
    verified
    .filter(F.col("shipped_date") >= F.lit("2023-07-30").cast("date"))
    .select("*", F.col("cost").cast("int").alias("cost_int"))
)

metrics = (
    valid_shipments
    .select(
        "*",
        F.sum("cost").over(w_grp).alias("status_sum_cost"),
        F.round((F.col("cost") / F.sum("cost").over(w2)), 4).alias("cost_share"),
        F.rank().over(w3).alias("cost_int_rank"),
        F.percent_rank().over(w_hist).alias("cost_int_percent_rank"),
    )
)

scored = (
    metrics
    .filter(F.col("status").isin(["RETURNED", "LOST", "IN_TRANSIT"]))
    .select(
        "*",
        F.dense_rank().over(w5).alias("cost_int_rank_dense_rank"),
        F.round((F.col("cost_int_rank") / F.sum("cost_int_rank").over(w2)), 4).alias("cost_int_rank_share"),
        F.percent_rank().over(w6).alias("cost_int_percent_rank_percent_rank"),
        F.rank().over(w7).alias("cost_rank"),
        F.rank().over(w8).alias("cost_int_rank_2"),
    )
)

scored_shipments = (
    scored
    .select(
        "id",
        "shipped_date",
        "status_carrier",
        "clean_status",
        "carrier_filled",
        "status_title",
        "cost_share",
        "cost_int_rank_share",
        "cost_int_percent_rank_percent_rank",
        "cost_rank",
        "cost_int_rank_2",
        F.dense_rank().over(w9).alias("cost_rank_dense_rank"),
        F.max("cost_int_rank_share").over(w10).alias("shipped_date_year_max_cost_int_rank_share"),
        F.dense_rank().over(w11).alias("cost_int_dense_rank"),
    )
)

totals = (
    scored_shipments
    .groupBy(
        F.when(F.col("cost_int_dense_rank") >= 8, "LARGE").otherwise("SMALL").alias("cost_int_dense_rank_tier"),
        F.date_trunc("MONTH", "shipped_date").alias("activity_month"),
    )
    .agg(
        F.count("*").alias("num_rows"),
        F.countDistinct("id").alias("distinct_ids"),
    )
    .filter(F.col("distinct_ids") > 1)
)

result = (
    totals
    .select("cost_int_dense_rank_tier", "activity_month", "num_rows")
    .orderBy(F.col("activity_month").desc_nulls_first())
)
