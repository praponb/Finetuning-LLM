from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("region", "city").orderBy("opened_date", "id")

cleaned = (
    df_stores
    .filter((F.col("opened_date") >= F.lit("2023-06-07").cast("date")) & F.col("region").isNotNull())
    .withColumn("store_name_upper", F.upper("store_name"))
    .withColumn("region_city", F.concat("region", F.lit(" - "), "city"))
    .withColumn("region_filled", F.coalesce("region", F.lit("DIRECT")))
)

enriched = (
    cleaned
    .filter(F.col("region") != "North")
    .select(
        "*",
        F.lead("sqft").over(w).alias("next_sqft"),
        F.lag("sqft").over(w).alias("prev_sqft"),
        F.row_number().over(w).alias("event_seq"),
    )
)

not_flagged = (
    enriched
    .alias("s")
    .join(df_store_sales.alias("x"), (F.col("x.store_id") == F.col("s.id")) & (F.col("x.sale_date") < F.col("s.opened_date")), "left_anti")
)

matched_keys = (
    not_flagged
    .select("city")
    .subtract(
        df_stores
        .filter(F.col("sqft") <= 8667)
        .select("city")
    )
)

verified = (
    not_flagged
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.city") == F.col("k.city")), "inner")
    .select("s.*")
)

eligible = (
    verified
    .alias("s")
    .join(df_inventory.alias("c"), (F.col("c.store_id") == F.col("s.id")) & (F.col("c.last_restock") >= F.col("s.opened_date")), "left_semi")
)

base = (
    eligible
    .filter((F.col("opened_date") >= F.lit("2023-03-16").cast("date")) & F.col("region").isNotNull())
    .select(
        "*",
        F.month("opened_date").alias("opened_date_month"),
        F.date_format("opened_date", "yyyy-MM").alias("opened_date_label"),
        F.coalesce("region", F.lit("NONE")).alias("region_clean"),
    )
)

not_flagged_stores = (
    base
    .alias("s")
    .join(df_orders.alias("c"), (F.col("c.store_id") == F.col("s.id")) & (F.col("c.amount") >= 1450), "left_anti")
)

result = (
    not_flagged_stores
    .withColumn("year_start", F.date_trunc("YEAR", "opened_date"))
    .cube("year_start", "region")
    .agg(
        F.size(F.collect_set("city")).alias("num_cities"),
        F.round(F.sum("next_sqft") / F.nullif(F.count(F.when(F.col("region") == "North", 1)), F.lit(0)), 2).alias("next_sqft_per_north"),
        F.grouping("year_start").alias("gid"),
    )
    .orderBy(F.col("num_cities").desc())
)
