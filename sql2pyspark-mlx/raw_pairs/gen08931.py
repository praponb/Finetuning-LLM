from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.partitionBy("channel", "store_id").orderBy("opened_date", "id")
w2 = Window.partitionBy("customer_id").orderBy(F.col("row_seq").asc_nulls_last(), "id")
w3 = Window.partitionBy("city").orderBy(F.col("prev_discount").asc_nulls_last(), "id")

enriched = (
    df_orders
    .alias("e")
    .join(df_stores.alias("b"), F.col("e.store_id").eqNullSafe(F.col("b.id")), "inner")
    .select("e.*", F.col("b.city"), F.col("b.opened_date"))
)

enriched_orders = (
    enriched
    .select(
        "id",
        "customer_id",
        "amount",
        "discount",
        "city",
        F.lead("discount").over(w_ordered).alias("next_discount"),
        F.lag("discount").over(w_ordered).alias("prev_discount"),
        F.lead("amount").over(w_ordered).alias("next_amount"),
        F.row_number().over(w_ordered).alias("row_seq"),
    )
)

deduped = (
    enriched_orders
    .withColumn("row_num", F.row_number().over(w2))
    .filter(F.col("row_num") == 1)
)

latest = (
    deduped
    .withColumn("latest_rank", F.row_number().over(w3))
    .filter(F.col("latest_rank") == 1)
)

remaining_keys = (
    latest
    .select("customer_id", "id")
    .subtract(
        df_orders
        .filter(F.col("shipping_fee") >= 20)
        .select("customer_id", "id")
    )
)

reconciled = (
    latest
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.customer_id") == F.col("k.customer_id")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .select(
        "id",
        "amount",
        "discount",
        "city",
        "prev_discount",
        "next_amount",
        "row_seq",
        "latest_rank",
        F.when(F.col("city").rlike("^(Chiang Mai|New York)$"), "EXTENDED").when(F.col("city").rlike("^(Bangkok|Singapore)$"), "CORE").otherwise("GROUP_B").alias("city_zone"),
    )
)
