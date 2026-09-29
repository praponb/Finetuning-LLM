from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.to_date("created_at")).orderBy(F.col("created_at").cast("bigint")).rangeBetween(-900, Window.currentRow)
w2 = Window.partitionBy(F.date_trunc("WEEK", "created_at"), "promo")
w3 = Window.partitionBy(F.year("created_at"), "customer_id")

lines = (
    df_carts
    .select("id", "customer_id", "created_at", "coupon_codes", F.inline("items"))
)

joined = (
    lines
    .alias("s")
    .join(df_customers.alias("p"), F.col("s.customer_id") == F.col("p.id"), "left")
    .filter(F.col("p.is_active"))
    .select(
        "s.id",
        "s.customer_id",
        "s.created_at",
        "s.price",
        "s.promo",
        F.col("p.name"),
    )
)

filtered = (
    joined
    .filter((F.col("promo").eqNullSafe(F.lit("VIP"))) & (F.col("created_at") >= F.lit("2025-03-02").cast("date")))
    .select(
        "id",
        "customer_id",
        "created_at",
        "price",
        "promo",
        "name",
        F.when(F.col("price") >= 217, "High").when(F.col("price") >= 55, "Medium").otherwise("Low").alias("price_tier"),
        F.coalesce("promo", F.lit("DIRECT")).alias("promo_clean"),
    )
)

enriched = (
    filtered
    .alias("s")
    .join(df_customers.alias("p"), F.col("s.customer_id") == F.col("p.id"), "left")
    .select("s.*", F.col("p.city"))
)

enriched_carts = (
    enriched
    .withColumn("price_max_15m", F.max("price").over(w))
    .withColumn("created_at_period_avg_price", F.avg("price").over(w2))
    .withColumn("price_share", F.round((F.col("price") / F.sum("price").over(w3)), 4))
)

result = (
    enriched_carts
    .select("city", "price_share", "price")
    .unpivot(
        ids=["city"],
        values=["price_share", "price"],
        variableColumnName="metric",
        valueColumnName="value",
    )
    .filter(F.col("value").isNotNull())
)
