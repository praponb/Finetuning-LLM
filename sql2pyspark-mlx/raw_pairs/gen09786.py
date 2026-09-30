from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy(F.col("created_at").cast("bigint")).rangeBetween(-7200, Window.currentRow)

lines = (
    df_carts
    .select("*", F.posexplode_outer("items").alias("pos", "elem"))
    .select(
        "id",
        "customer_id",
        "created_at",
        "pos",
        F.col("elem.product_id"),
        F.col("elem.qty"),
        F.col("elem.price"),
        F.col("elem.promo"),
    )
)

valid = (
    lines
    .filter((~F.col("promo").eqNullSafe(F.lit("SPRING10"))) & (F.col("created_at") > F.lit("2025-03-02").cast("date")))
    .withColumn("promo_upper", F.upper("promo"))
    .withColumn("promo_filled", F.coalesce("promo", F.lit("NONE")))
)

metrics = (
    valid
    .select(
        "created_at",
        "pos",
        "price",
        "promo",
        "promo_filled",
        F.sum("price").over(w).alias("price_sum_2h"),
    )
)

cleaned = (
    metrics
    .filter(F.col("price").between(190, 200))
    .withColumn("created_at_hour", F.hour("created_at"))
)

filtered = (
    cleaned
    .filter((F.col("created_at") >= F.lit("2025-03-01").cast("date")) & F.col("promo").isNotNull())
    .select(
        "*",
        (F.col("price_sum_2h") - F.col("created_at_hour")).alias("price_sum_2h_minus_created_at_hour"),
        F.coalesce("promo", F.lit("N/A")).alias("resolved_promo"),
    )
)

result = (
    filtered
    .select(
        "created_at",
        "pos",
        "promo",
        "promo_filled",
        "price_sum_2h",
        "created_at_hour",
        "price_sum_2h_minus_created_at_hour",
        "resolved_promo",
    )
    .orderBy(F.col("created_at_hour").desc_nulls_first())
)
