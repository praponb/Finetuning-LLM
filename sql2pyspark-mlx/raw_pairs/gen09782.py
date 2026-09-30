from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("k", "customer_id").orderBy("created_at", "id", "k")
w2 = Window.partitionBy("k", "customer_id").orderBy(F.col("created_at").desc(), "id", "k")
w3 = Window.partitionBy("k").orderBy("created_at", "id", "k")
w4 = Window.partitionBy("k").orderBy("created_at", "id", "k").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.orderBy(F.col("created_at").desc(), "id", "k")

exploded = (
    df_carts
    .select(
        "id",
        "customer_id",
        "created_at",
        "coupon_codes",
        F.explode("attributes").alias("k", "attr_value"),
    )
)

deduped = (
    exploded
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
    .select("id", "customer_id", "created_at", "k")
)

latest = (
    deduped
    .withColumn("rank_in_group", F.row_number().over(w2))
    .filter(F.col("rank_in_group") == 1)
    .drop("rank_in_group")
)

enriched = (
    latest
    .select(
        "id",
        "customer_id",
        "k",
        F.row_number().over(w3).alias("row_seq"),
        F.first("k", ignorenulls=True).over(w4).alias("first_known_k"),
        F.row_number().over(w5).alias("seq_no"),
    )
)

result = (
    enriched
    .groupBy(
        F.when(F.col("row_seq") >= 7, "PREMIUM").otherwise("STANDARD").alias("row_seq_tier"),
        "customer_id",
        "k",
    )
    .agg(
        F.round(F.sum("row_seq") / F.nullif(F.count(F.when(F.col("k") == "utm_source", 1)), F.lit(0)), 2).alias("row_seq_per_utm_source"),
        F.sum(F.when(F.col("k") == "gift", F.col("seq_no")).otherwise(0)).alias("gift_seq_no"),
    )
    .orderBy(F.col("customer_id").desc())
)
