from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy(F.col("rating").desc(), "id")
w2 = Window.partitionBy("country").orderBy(F.col("rating").desc(), "id")
w3 = Window.orderBy(F.col("rating").desc(), "id")
w4 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.cost").desc(), F.col("c.id").desc())
w5 = Window.partitionBy("country")
w6 = Window.partitionBy(F.month("launch_date"), "city")
w7 = Window.partitionBy("country").orderBy("event_seq", "id")
w8 = Window.partitionBy(F.month("launch_date"), "city").orderBy("rating_bucket", "id")
w9 = Window.partitionBy("city").orderBy(F.col("rating_bucket_share").desc(), "id")

base = (
    df_suppliers
    .filter(F.col("city").isin(["Singapore", "Tokyo"]))
    .withColumn("city_lower", F.lower("city"))
    .withColumn("city_country", F.concat("city", F.lit(" - "), "country"))
)

metrics = (
    base
    .withColumn("seq_no", F.row_number().over(w))
    .withColumn("event_seq", F.row_number().over(w2))
    .withColumn("rating_bucket", F.ntile(10).over(w3))
)

with_latest = (
    metrics
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w4))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.launch_date"), F.col("c.stock_qty"))
)

metrics_suppliers = (
    with_latest
    .select(
        "*",
        F.round((F.col("rating_bucket") / F.sum("rating_bucket").over(w5)), 4).alias("rating_bucket_share"),
        F.round((F.col("event_seq") / F.sum("event_seq").over(w6)), 4).alias("event_seq_share"),
        F.lead("stock_qty", 1).over(w7).alias("next_stock_qty"),
        F.ntile(5).over(w8).alias("rating_bucket_bucket"),
    )
)

filtered = (
    metrics_suppliers
    .filter(F.col("city").isNotNull() & (F.col("rating_bucket_bucket") != 7))
    .select(
        "*",
        (F.col("event_seq_share") + F.col("rating_bucket_share")).alias("event_seq_share_plus_rating_bucket_share"),
        F.round("next_stock_qty", 1).alias("next_stock_qty_rounded"),
        F.coalesce("city", F.lit("N/A")).alias("resolved_city"),
    )
)

deduped = (
    filtered
    .withColumn("rn", F.row_number().over(w9))
    .filter(F.col("rn") == 1)
)

result = (
    deduped
    .withColumn("year_start", F.date_trunc("YEAR", "launch_date"))
    .groupingSets(
        [["city", "year_start", "country"], ["city", "year_start"]],
        "city",
        "year_start",
        "country",
    )
    .agg(
        F.sort_array(F.collect_set("city")).alias("city_list"),
        F.percentile_approx("next_stock_qty", 0.9).alias("next_stock_qty_p90"),
        F.count(F.when(F.col("country") == "UK", 1)).alias("uk_rows"),
    )
)
