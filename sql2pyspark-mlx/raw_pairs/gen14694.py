from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.last_restock").desc(), F.col("c.id").desc())
w2 = Window.orderBy("sqft_filled", "id")
w3 = Window.partitionBy(F.date_trunc("month", "opened_date")).orderBy(F.col("sqft_filled").desc())
w4 = Window.orderBy("quantity", "id")
w5 = Window.orderBy("quantity", "id").rowsBetween(-6, -1)
w6 = Window.orderBy("sqft_filled", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.orderBy("quantity_avg_6_prior", "id")
w8 = Window.orderBy(F.col("prev_sqft_filled").asc_nulls_first())
w9 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.discount").desc(), F.col("c.id").desc())

cleaned = (
    df_stores
    .filter(F.col("store_name").like("Sia%"))
    .select(
        "id",
        "store_name",
        "city",
        "region",
        "opened_date",
        "sqft",
        F.initcap(F.trim("region")).alias("region_title"),
        F.coalesce("sqft", F.lit(0)).alias("sqft_filled"),
    )
)

cleaned_stores = (
    cleaned
    .filter((F.col("opened_date") >= F.to_date(F.lit("2023-07-06"))) & F.col("store_name").like("%Express%"))
    .select(
        "id",
        "store_name",
        "city",
        "region",
        "opened_date",
        "region_title",
        "sqft_filled",
        F.regexp_replace("store_name", " ", "-").alias("store_name_slug"),
    )
)

with_last = (
    cleaned_stores
    .alias("s")
    .join(df_inventory.alias("c"), (F.col("c.store_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.store_name",
        "s.opened_date",
        "s.sqft_filled",
        F.col("c.quantity"),
    )
)

scored = (
    with_last
    .select(
        "id",
        "sqft_filled",
        F.row_number().over(w2).alias("seq_no"),
        F.rank().over(w3).alias("sqft_filled_rank"),
        F.lead("quantity", 1).over(w4).alias("next_quantity"),
        F.lag("sqft_filled", 1).over(w2).alias("prev_sqft_filled"),
        F.avg("quantity").over(w5).alias("quantity_avg_6_prior"),
    )
)

enriched = (
    scored
    .filter(F.col("sqft_filled") <= 25)
    .select(
        "*",
        F.last("prev_sqft_filled", ignorenulls=True).over(w6).alias("last_known_prev_sqft_filled"),
        F.lead("sqft_filled", 1).over(w7).alias("next_sqft_filled"),
        F.cume_dist().over(w8).alias("prev_sqft_filled_cume_dist"),
    )
)

with_last_stores = (
    enriched
    .alias("s")
    .join(df_orders.alias("c"), (F.col("c.store_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w9))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.discount"), F.col("c.channel").alias("order_channel"))
)

result = (
    with_last_stores
    .rollup(
        F.when(F.col("quantity_avg_6_prior") >= 203, "PREMIUM").otherwise("STANDARD").alias("quantity_avg_6_prior_tier"),
        F.when(F.col("seq_no") >= 8, "HIGH").otherwise("LOW").alias("seq_no_tier"),
    )
    .agg(
        F.count(F.when(F.col("order_channel") == "partner", 1)).alias("partner_rows"),
        F.round(F.sum("quantity_avg_6_prior") / F.nullif(F.count(F.when(F.col("order_channel") == "web", 1)), F.lit(0)), 2).alias("quantity_avg_6_prior_per_web"),
        F.round(F.avg("last_known_prev_sqft_filled"), 2).alias("avg_last_known_prev_sqft_filled"),
        F.count("*").alias("num_rows"),
        F.round(F.avg("sqft_filled"), 2).alias("avg_sqft_filled"),
    )
    .filter(F.col("avg_sqft_filled") < 1000000)
)
