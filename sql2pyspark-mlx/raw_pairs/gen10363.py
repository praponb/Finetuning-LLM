from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.month("created_at"), "promo").orderBy(F.col("created_at").cast("bigint")).rangeBetween(-3600, Window.currentRow)
w2 = Window.partitionBy(F.month("created_at"), "promo")
w3 = Window.partitionBy("promo")

flattened = (
    df_carts
    .withColumn("line", F.explode("items"))
    .select(
        "id",
        "created_at",
        F.col("line.qty"),
        F.col("line.price"),
        F.col("line.promo"),
    )
)

base = (
    flattened
    .filter(F.col("promo").isNotNull())
    .select(
        "id",
        "created_at",
        "qty",
        "promo",
        F.hour("created_at").alias("created_at_hour"),
        (F.datediff(F.current_date(), "created_at") / 365).alias("years_since_created_at"),
    )
)

filtered = (
    base
    .filter((F.col("created_at") >= F.lit("2025-03-01").cast("date")) & (F.col("created_at_hour") <= 27))
    .withColumn("qty_per_created_at_hour", (F.col("qty") / F.nullif("created_at_hour", F.lit(0))))
    .withColumn("promo_lower", F.lower("promo"))
)

scored = (
    filtered
    .filter(F.col("created_at") < "2024-03-11")
    .withColumn("years_since_created_at_avg_1h", F.avg("years_since_created_at").over(w))
    .withColumn("created_at_month_max_qty", F.max("qty").over(w2))
    .withColumn("promo_avg_qty_per_created_at_hour", F.avg("qty_per_created_at_hour").over(w3))
)

result = (
    scored
    .select("promo", "qty_per_created_at_hour", "years_since_created_at")
    .unpivot(
        ids=["promo"],
        values=["qty_per_created_at_hour", "years_since_created_at"],
        variableColumnName="measure",
        valueColumnName="reading",
    )
    .filter(F.col("reading").isNotNull())
)
