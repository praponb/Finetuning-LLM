from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel", "customer_id").orderBy(F.col("shipping_fee").desc(), "id")
w2 = Window.partitionBy("customer_id").orderBy(F.col("amount").desc(), "id")

valid = (
    df_orders
    .filter((F.col("order_date") >= F.lit("2023-04-22").cast("date")) & (F.col("customer_id") == 22))
    .select("*", F.concat_ws(",", "channel", "status").alias("channel_status_key"))
)

ranked = (
    valid
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "id",
        "customer_id",
        "store_id",
        "order_date",
        "status",
        "channel",
        "amount",
    )
)

deduped = (
    ranked
    .withColumn("rank_in_group", F.row_number().over(w2))
    .filter(F.col("rank_in_group") == 1)
    .drop("rank_in_group")
)

matched = (
    deduped
    .alias("s")
    .join(df_promotions.alias("pr"), (F.col("s.channel").eqNullSafe(F.col("pr.channel"))) & (F.col("s.order_date") >= F.col("pr.starts_on")) & (F.col("s.order_date") < F.col("pr.ends_on") + F.expr("INTERVAL 14 DAYS")), "left")
    .select(
        "s.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code"),
        F.col("pr.discount_pct"),
    )
)

result = (
    matched
    .select(
        "id",
        "customer_id",
        "store_id",
        "order_date",
        "status",
        "channel",
        "amount",
        "promo_id",
        F.when(F.col("channel").rlike("^(partner|store)$"), "GROUP_A").otherwise("GROUP_B").alias("channel_zone"),
    )
    .orderBy("customer_id")
)
