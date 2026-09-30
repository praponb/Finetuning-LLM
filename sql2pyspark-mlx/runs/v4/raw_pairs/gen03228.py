from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("sqft").asc_nulls_first())
w2 = Window.partitionBy("region", "city").orderBy("sqft", "id")
w3 = Window.orderBy(F.col("sqft").desc_nulls_last())
w4 = Window.partitionBy("region", "city").orderBy(F.col("sqft_bucket").desc(), "id")
w5 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.revenue").desc(), F.col("c.id").desc())
w6 = Window.partitionBy("city").orderBy("sqft_bucket", "id")
w_hist = Window.partitionBy("city").orderBy("sqft", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.partitionBy("city")
w9 = Window.partitionBy("region", "city").orderBy(F.col("sqft").desc(), "id")
w10 = Window.partitionBy("city").orderBy("sqft_bucket")

filtered = (
    df_stores
    .filter((F.col("opened_date") >= F.lit("2023-03-26").cast("date")) & F.col("region").isNotNull())
    .select(
        "*",
        F.when(F.col("sqft") >= 17422, "High").when(F.col("sqft") >= 4942, "Medium").otherwise("Low").alias("sqft_level"),
        F.coalesce("region", F.lit("UNKNOWN")).alias("region_clean"),
    )
)

with_windows = (
    filtered
    .withColumn("sqft_rank", F.rank().over(w))
    .withColumn("sqft_bucket", F.ntile(3).over(w2))
    .withColumn("sqft_cume_dist", F.cume_dist().over(w3))
)

ranked = (
    with_windows
    .withColumn("latest_rank", F.row_number().over(w4))
    .filter(F.col("latest_rank") == 1)
    .drop("latest_rank")
)

with_latest = (
    ranked
    .alias("s")
    .join(df_store_sales.alias("c"), (F.col("c.store_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w5))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.units"), F.col("c.revenue").alias("store_sale_revenue"))
)

enriched = (
    with_latest
    .withColumn("next_sqft_rank", F.lead("sqft_rank", 1).over(w6))
    .withColumn("running_sqft_rank", F.sum("sqft_rank").over(w_hist))
    .withColumn("city_count_sqft", F.count("sqft").over(w8))
    .withColumn("event_seq", F.row_number().over(w9))
)

ranked_stores = (
    enriched
    .withColumn("rank_in_group", F.rank().over(w10))
    .filter(F.col("rank_in_group") == 1)
)

matched_keys = (
    ranked_stores
    .select("city", "id")
    .subtract(
        df_stores
        .filter(F.length("store_name") < 5)
        .select("city", "id")
    )
)

kept_rows = (
    ranked_stores
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.city") == F.col("k.city")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .select(
        "id",
        "store_name",
        "region",
        "opened_date",
        "sqft",
        "sqft_rank",
        "sqft_cume_dist",
        "units",
        "next_sqft_rank",
        F.when(F.col("region").rlike("^(East|Central)$"), "CORE").when(F.col("region").rlike("^(North|South)$"), "GROUP_B").otherwise("OTHER_ZONE").alias("region_zone"),
    )
    .orderBy(F.col("next_sqft_rank").desc_nulls_last())
)
