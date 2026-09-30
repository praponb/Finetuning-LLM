from pyspark.sql import functions as F

lines = (
    df_price_tiers
    .select(
        "id",
        "effective_from",
        "currency",
        F.explode_outer("tier_prices").alias("element"),
    )
)

result = (
    lines
    .select("id", "effective_from", "currency")
    .orderBy(F.col("effective_from").asc_nulls_first())
)
