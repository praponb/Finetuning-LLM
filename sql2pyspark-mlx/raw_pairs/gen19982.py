from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("k").orderBy(F.col("created_at").desc(), "id", "k")
w_grp = Window.orderBy(F.col("created_at").desc(), "id", "k")
w3 = Window.partitionBy(F.month("created_at"), "k").orderBy("created_at", "id", "k").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("k").orderBy("created_at", "id", "k")
w_running = Window.orderBy("created_at", "id", "k").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("k").orderBy(F.col("seq_no").desc_nulls_first())
w7 = Window.partitionBy("k").orderBy("seq_no_2_minus_dedup_rank")
w8 = Window.partitionBy("k").orderBy("dedup_rank_per_seq_no_2", "id", "k")
w9 = Window.orderBy(F.col("dedup_rank").desc(), "id", "k")

lines = df_carts.select("id", "created_at", F.explode("attributes").alias("k", "value"))

metrics = (
    lines
    .select(
        "id",
        "k",
        "value",
        F.row_number().over(w).alias("row_seq"),
        F.row_number().over(w_grp).alias("seq_no"),
        F.first("k", ignorenulls=True).over(w3).alias("first_known_k"),
        F.row_number().over(w4).alias("seq_no_2"),
        F.last("k", ignorenulls=True).over(w_running).alias("last_known_k"),
    )
)

ranked = (
    metrics
    .withColumn("dedup_rank", F.rank().over(w6))
    .filter(F.col("dedup_rank") == 1)
)

base = (
    ranked
    .filter(F.col("k").isNotNull())
    .withColumn("dedup_rank_whole", F.ceil("dedup_rank"))
)

base_carts = (
    base
    .filter(F.col("seq_no") >= 7)
    .select(
        "id",
        "k",
        "value",
        "row_seq",
        "seq_no",
        "last_known_k",
        "dedup_rank",
        F.round((F.col("dedup_rank") / F.col("seq_no_2")), 2).alias("dedup_rank_per_seq_no_2"),
        (F.col("seq_no_2") - F.col("dedup_rank")).alias("seq_no_2_minus_dedup_rank"),
    )
)

result = (
    base_carts
    .select(
        "id",
        "k",
        "row_seq",
        "dedup_rank_per_seq_no_2",
        "seq_no_2_minus_dedup_rank",
        F.cume_dist().over(w7).alias("seq_no_2_minus_dedup_rank_cume_dist"),
        F.lead("dedup_rank_per_seq_no_2", 1).over(w8).alias("next_dedup_rank_per_seq_no_2"),
        F.row_number().over(w9).alias("event_seq"),
    )
)
