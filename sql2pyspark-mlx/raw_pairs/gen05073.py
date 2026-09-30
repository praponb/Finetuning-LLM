from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.partitionBy("carrier").orderBy("shipped_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("status").orderBy(F.col("running_min_cost").desc_nulls_first(), "id")
w3 = Window.orderBy(F.col("running_min_cost").asc_nulls_last())
w4 = Window.orderBy(F.col("running_sum_cost").desc_nulls_last())
w5 = Window.orderBy(F.col("running_min_cost").desc())
w6 = Window.partitionBy(F.date_trunc("MONTH", "shipped_date")).orderBy(F.col("running_count_cost").desc_nulls_first())
w7 = Window.orderBy(F.col("running_count_cost").desc_nulls_last())
w8 = Window.orderBy(F.col("running_avg_cost").desc_nulls_last())
w9 = Window.orderBy(F.col("running_avg_cost").asc_nulls_last())
w10 = Window.orderBy(F.col("running_count_cost_rank_2").desc_nulls_first())

filtered = (
    df_shipments
    .filter(F.col("order_id") == 7)
    .select(
        "id",
        "carrier",
        "shipped_date",
        "cost",
        "status",
        F.concat_ws("/", "carrier", "status").alias("carrier_status_key"),
        F.coalesce("carrier", F.lit("NONE")).alias("carrier_filled"),
    )
)

scored = (
    filtered
    .filter(F.col("status") == "IN_TRANSIT")
    .select(
        "id",
        "shipped_date",
        "status",
        "carrier_status_key",
        F.avg("cost").over(w_grp).alias("running_avg_cost"),
        F.min("cost").over(w_grp).alias("running_min_cost"),
        F.count("cost").over(w_grp).alias("running_count_cost"),
        F.sum("cost").over(w_grp).alias("running_sum_cost"),
    )
)

deduped = (
    scored
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

matched_keys = (
    deduped
    .select("id")
    .intersect(
        df_shipments
        .filter(F.col("status") == "LOST")
        .select("id")
    )
)

reconciled = (
    deduped
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

base = (
    reconciled
    .filter(F.col("shipped_date") > F.lit("2023-04-24").cast("date"))
    .select(
        "id",
        "shipped_date",
        "carrier_status_key",
        "running_avg_cost",
        "running_min_cost",
        "running_count_cost",
        "running_sum_cost",
        F.trim("status").alias("clean_status"),
    )
)

enriched = (
    base
    .select(
        "running_avg_cost",
        "running_count_cost",
        F.dense_rank().over(w3).alias("running_min_cost_dense_rank"),
        F.rank().over(w4).alias("running_sum_cost_rank"),
        F.dense_rank().over(w5).alias("running_min_cost_dense_rank_2"),
        F.rank().over(w6).alias("running_count_cost_rank"),
        F.rank().over(w7).alias("running_count_cost_rank_2"),
    )
)

metrics = (
    enriched
    .select(
        "*",
        F.cume_dist().over(w8).alias("running_avg_cost_cume_dist"),
        F.rank().over(w9).alias("running_avg_cost_rank"),
        F.rank().over(w10).alias("running_count_cost_rank_2_rank"),
    )
)

result = (
    metrics
    .select(
        "running_avg_cost",
        "running_count_cost",
        "running_sum_cost_rank",
        "running_min_cost_dense_rank_2",
        "running_count_cost_rank",
        "running_count_cost_rank_2",
        "running_avg_cost_cume_dist",
        "running_avg_cost_rank",
        "running_count_cost_rank_2_rank",
    )
    .orderBy(F.col("running_count_cost_rank_2").asc_nulls_first())
)
