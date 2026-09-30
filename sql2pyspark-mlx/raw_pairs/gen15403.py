from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.amount").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("region", "city").orderBy(F.col("sqft").desc())
w3 = Window.orderBy("sqft", "id")

base = (
    df_stores
    .filter((F.col("opened_date") > F.lit("2023-04-18").cast("date")) & (~F.col("region").eqNullSafe(F.lit("West"))))
    .select(
        "*",
        F.trim("region").alias("clean_region"),
        F.when(F.col("region").isin(["North", "Central"]), "Tier 1").otherwise("Other").alias("region_group"),
    )
)

latest_match = (
    base
    .alias("s")
    .join(df_orders.alias("c"), (F.col("c.store_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.shipping_fee"))
)

enriched = (
    latest_match
    .select(
        "*",
        F.percent_rank().over(w2).alias("sqft_percent_rank"),
        F.row_number().over(w3).alias("row_seq"),
    )
)

cleaned = (
    enriched
    .filter((F.col("opened_date") > F.lit("2023-09-23").cast("date")) & F.col("region").isNotNull())
    .select(
        "id",
        "city",
        "region",
        "sqft",
        "sqft_percent_rank",
        F.substring("store_name", 1, 4).alias("store_name_prefix"),
        F.coalesce("region", F.lit("UNKNOWN")).alias("resolved_region"),
    )
)

kept = (
    cleaned
    .alias("s")
    .join(df_store_sales.alias("x"), (F.col("x.store_id") == F.col("s.id")) & (F.col("x.store_id") == 6), "left_anti")
)

ranked = (
    kept
    .withColumn("rn", F.rank().over(w2))
    .filter(F.col("rn") == 1)
    .select(
        "id",
        "region",
        "sqft",
        "sqft_percent_rank",
        "store_name_prefix",
        "resolved_region",
    )
)

result = (
    ranked
    .select(
        "id",
        "region",
        "sqft",
        "sqft_percent_rank",
        "store_name_prefix",
        "resolved_region",
        F.when(F.col("region").rlike("^(West|North)$"), "OTHER_ZONE").otherwise("GROUP_B").alias("region_zone"),
    )
    .orderBy(F.col("sqft").asc_nulls_first(), F.col("id").desc())
)
