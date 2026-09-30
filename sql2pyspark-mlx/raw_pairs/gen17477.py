from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("cost").desc(), "id")
w2 = Window.orderBy("delivered_date", "id")
w3 = Window.orderBy(F.col("delivered_date").desc(), "id")
w4 = Window.orderBy(F.col("seq_no").desc_nulls_last())
w5 = Window.partitionBy(F.date_trunc("WEEK", "delivered_date"))
w6 = Window.orderBy(F.col("seq_no_dense_rank").desc())
w_ordered = Window.orderBy(F.col("seq_no").desc_nulls_first())
w8 = Window.partitionBy(F.month("delivered_date")).orderBy(F.col("seq_no_dense_rank_share").desc())

valid = (
    df_shipments
    .filter((F.col("delivered_date") > F.to_date(F.lit("2023-07-28"))) & F.col("order_id").isNotNull())
    .select(
        "id",
        "shipped_date",
        "delivered_date",
        "cost",
        "status",
        F.coalesce("carrier", F.lit("Unknown")).alias("carrier_filled"),
    )
)

ranked = (
    valid
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
    .select("id", "delivered_date")
)

scored = (
    ranked
    .withColumn("seq_no", F.row_number().over(w2))
    .withColumn("row_seq", F.row_number().over(w3))
    .withColumn("row_seq_2", F.row_number().over(w2))
)

kept_keys = (
    scored
    .select("id")
    .intersectAll(
        df_shipments
        .filter(~F.col("carrier").isin(["UPS", "FedEx", "DHL"]))
        .select("id")
    )
)

reconciled = (
    scored
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

with_windows = (
    reconciled
    .select(
        "id",
        "delivered_date",
        "seq_no",
        F.dense_rank().over(w4).alias("seq_no_dense_rank"),
    )
)

enriched = (
    with_windows
    .select(
        "id",
        "delivered_date",
        F.round((F.col("seq_no_dense_rank") / F.sum("seq_no_dense_rank").over(w5)), 4).alias("seq_no_dense_rank_share"),
        F.percent_rank().over(w6).alias("seq_no_dense_rank_percent_rank"),
        F.rank().over(w4).alias("seq_no_rank"),
        F.rank().over(w_ordered).alias("seq_no_rank_2"),
    )
)

metrics = (
    enriched
    .filter(F.col("delivered_date").between("2023-10-26", "2023-10-30"))
    .select("*", F.rank().over(w8).alias("seq_no_dense_rank_share_rank"))
)

result = (
    metrics
    .select(
        "id",
        "delivered_date",
        "seq_no_dense_rank_share",
        "seq_no_dense_rank_percent_rank",
        "seq_no_rank",
        "seq_no_rank_2",
    )
)
