from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.sale_date").desc(), F.col("c.id").desc())
w2 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.quantity").desc(), F.col("c.id").desc())
w3 = Window.orderBy("last_restock", "id")
w4 = Window.orderBy(F.col("store_sale_sale_date").desc(), "id")

filtered = (
    df_products
    .filter(F.col("launch_date") > F.lit("2023-06-02").cast("date"))
    .select(
        "id",
        "product_name",
        "price",
        "stock_qty",
        "launch_date",
        F.concat_ws(",", "product_name", "category").alias("product_name_category_key"),
        F.coalesce("brand", F.lit("DIRECT")).alias("resolved_brand"),
    )
)

with_latest = (
    filtered
    .alias("s")
    .join(df_store_sales.alias("c"), (F.col("c.product_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.product_name_category_key",
        "s.resolved_brand",
        F.col("c.units").alias("store_sale_units"),
        F.col("c.sale_date").alias("store_sale_sale_date"),
    )
)

latest_match = (
    with_latest
    .alias("s")
    .join(df_inventory.alias("c"), (F.col("c.product_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.last_restock"))
)

scored = (
    latest_match
    .withColumn("next_store_sale_units", F.lead("store_sale_units", 1, 0).over(w3))
    .withColumn("event_seq", F.row_number().over(w4))
)

result = (
    scored
    .withColumn("event_seq_tier", F.when(F.col("event_seq") >= 8, "LARGE").otherwise("SMALL"))
    .withColumn("week_start", F.date_trunc("WEEK", "store_sale_sale_date"))
    .withColumn("year_start", F.date_trunc("YEAR", "store_sale_sale_date"))
    .groupingSets(
        [["event_seq_tier", "week_start", "year_start"], ["event_seq_tier", "week_start"]],
        "event_seq_tier",
        "week_start",
        "year_start",
    )
    .agg(
        F.percentile_approx("event_seq", 0.5).alias("event_seq_p50"),
        F.count("*").alias("cnt"),
        F.round(F.avg("next_store_sale_units"), 2).alias("avg_next_store_sale_units"),
        F.grouping("event_seq_tier").alias("grouping_level"),
    )
)
