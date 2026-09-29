from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("order_date", "id").rowsBetween(-5, Window.currentRow)
w2 = Window.partitionBy("status", "customer_id")
w3 = Window.partitionBy("customer_id").orderBy(F.col("discount_avg_5").asc_nulls_last())

metrics = (
    df_orders
    .withColumn("discount_avg_5", F.avg("discount").over(w))
    .withColumn("corr_discount_amount", F.corr("discount", "amount").over(w2))
)

ranked = (
    metrics
    .withColumn("rn", F.rank().over(w3))
    .filter(F.col("rn") == 1)
    .select(
        "id",
        "order_date",
        "status",
        "channel",
        "discount_avg_5",
        "corr_discount_amount",
    )
)

cleaned = (
    ranked
    .filter((~F.col("channel").eqNullSafe(F.lit("mobile"))) & (F.col("order_date") >= F.lit("2023-08-18").cast("date")))
    .withColumn("order_date_label", F.date_format("order_date", "yyyy-MM-dd"))
    .withColumn("status_group", F.when(F.col("status").isin(["SHIPPED", "CANCELLED"]), "Core").otherwise("Other"))
    .withColumn("resolved_channel", F.coalesce("channel", F.lit("DIRECT")))
)

matched = (
    cleaned
    .alias("src")
    .join(df_promotions.alias("pr"), (F.col("src.channel").eqNullSafe(F.col("pr.channel"))) & (F.col("src.order_date") >= F.col("pr.starts_on")) & (F.col("src.order_date") < F.col("pr.ends_on") + F.expr("INTERVAL 7 DAYS")), "left")
    .select(
        "src.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code"),
        F.col("pr.discount_pct"),
    )
)

result = (
    matched
    .select(
        "id",
        "order_date",
        "status",
        "channel",
        "discount_avg_5",
        "order_date_label",
        "resolved_channel",
        "promo_id",
        "promo_code",
    )
)
