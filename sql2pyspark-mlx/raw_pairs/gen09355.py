from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("brand").orderBy(F.col("launch_date").asc_nulls_last(), "id")
w2 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.unit_price").desc(), F.col("c.id").desc())
w3 = Window.partitionBy("brand").orderBy(F.col("launch_date").asc_nulls_last())
w4 = Window.partitionBy("brand").orderBy(F.col("price").desc_nulls_last(), "id")

valid = (
    df_products
    .filter((F.col("price") < 630) & F.col("supplier_id").isNotNull())
    .withColumn("margin", (F.col("cost") - F.col("price")))
    .withColumn("resolved_brand", F.coalesce("brand", F.lit("UNKNOWN")))
)

deduped = (
    valid
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "id",
        "product_name",
        "brand",
        "price",
        "cost",
        "launch_date",
        "resolved_brand",
    )
)

with_latest = (
    deduped
    .alias("s")
    .join(df_order_items.alias("c"), (F.col("c.product_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.quantity"))
)

ranked = (
    with_latest
    .withColumn("dedup_rank", F.rank().over(w3))
    .filter(F.col("dedup_rank") == 1)
)

latest = (
    ranked
    .withColumn("rank_in_group", F.row_number().over(w4))
    .filter(F.col("rank_in_group") == 1)
    .select("id", "product_name", "price", "launch_date", "resolved_brand")
)

result = (
    latest
    .rollup(
        F.date_trunc("YEAR", "launch_date").alias("year_start"),
        F.date_trunc("QUARTER", "launch_date").alias("quarter_start"),
        F.when(F.col("price") >= 900, "PREMIUM").otherwise("STANDARD").alias("price_tier"),
    )
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.count("*").alias("row_count"),
    )
    .filter(F.col("distinct_ids") >= 1)
)
