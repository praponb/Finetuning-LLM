from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id", "status").orderBy(F.col("cost").desc_nulls_last())
w2 = Window.orderBy(F.col("cost").asc_nulls_first())
w3 = Window.partitionBy("status").orderBy(F.col("amount").asc_nulls_last(), "id")

enriched = (
    df_shipments
    .select(
        "id",
        "order_id",
        "carrier",
        "shipped_date",
        "status",
        F.rank().over(w).alias("cost_rank"),
        F.rank().over(w2).alias("cost_rank_2"),
    )
)

joined = (
    enriched
    .alias("e")
    .join(df_orders.alias("b"), F.col("e.order_id") == F.col("b.id"), "left")
    .select("e.*", F.col("b.amount"))
)

ranked = (
    joined
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
)

remaining_keys = (
    ranked
    .select("id", "order_id")
    .intersect(
        df_shipments
        .filter(F.col("status") == "RETURNED")
        .select("id", "order_id")
    )
)

verified = (
    ranked
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.order_id") == F.col("k.order_id")), "inner")
    .select("s.*")
)

result = (
    verified
    .select("id", "carrier", "shipped_date", "amount")
    .orderBy(F.col("carrier").asc_nulls_first())
)
