from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.date_trunc("YEAR", "created_at"), "customer_id")

exploded = (
    df_carts
    .withColumn("line", F.explode_outer("items"))
    .select(
        "id",
        "customer_id",
        "created_at",
        F.col("line.product_id"),
        F.col("line.qty"),
        F.col("line.price"),
        F.col("line.promo"),
    )
)

metrics = (
    exploded
    .select(
        "*",
        F.round((F.col("price") / F.sum("price").over(w)), 4).alias("price_share"),
    )
)

base = (
    metrics
    .filter((F.col("promo").eqNullSafe(F.lit("VIP"))) & (F.col("qty") != 5))
    .select("*", F.round((F.col("price") * 100), 2).alias("adjusted_price"))
)

joined = (
    base
    .alias("src")
    .join(df_customers.alias("d"), F.col("src.customer_id") == F.col("d.id"), "left")
    .filter(~F.col("d.is_active"))
    .select(
        "src.customer_id",
        "src.created_at",
        "src.qty",
        "src.promo",
        "src.price_share",
        "src.adjusted_price",
        F.col("d.name"),
        F.col("d.country"),
        F.col("d.credit_limit"),
    )
)

by_type = (
    joined
    .groupBy("promo")
    .pivot("country", ["TH", "JP"])
    .agg(F.avg("qty").alias("total"))
)

result = (
    by_type
    .select(
        "promo",
        "TH",
        "JP",
        F.when(F.col("promo").rlike("^(WELCOME5|BUNDLE)$"), "GROUP_A").when(F.col("promo").rlike("^(SPRING10|VIP)$"), "EXTENDED").otherwise("PRIORITY").alias("promo_zone"),
    )
    .orderBy("TH")
)
