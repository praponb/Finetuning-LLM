from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.revenue").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("region").orderBy(F.col("opened_date").desc())

eligible = (
    df_stores
    .alias("s")
    .join(df_orders.alias("c"), (F.col("c.store_id") == F.col("s.id")) & (F.col("c.status") != "RETURNED"), "left_anti")
)

base = (
    eligible
    .filter((F.col("opened_date") >= F.lit("2023-02-25").cast("date")) & F.col("region").isNotNull())
    .select(
        "id",
        "store_name",
        "region",
        "opened_date",
        "sqft",
        F.regexp_replace("store_name", " ", "").alias("store_name_slug"),
        F.coalesce("region", F.lit("UNKNOWN")).alias("resolved_region"),
    )
)

with_latest = (
    base
    .alias("s")
    .join(df_store_sales.alias("c"), (F.col("c.store_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.sale_date"))
)

latest = (
    with_latest
    .withColumn("dedup_rank", F.rank().over(w2))
    .filter(F.col("dedup_rank") == 1)
    .drop("dedup_rank")
)

filtered = (
    latest
    .filter(F.col("region").isNotNull())
    .select(
        "id",
        "store_name",
        "region",
        "opened_date",
        "sqft",
        "store_name_slug",
        "sale_date",
        F.ceil("sqft").alias("sqft_whole"),
        F.date_format("opened_date", "yyyy-MM-dd").alias("opened_date_label"),
    )
)

result = (
    filtered
    .groupBy(
        "region",
        F.date_trunc("QUARTER", "sale_date").alias("quarter_start"),
        F.when(F.col("sqft") >= 12621, "HIGH").otherwise("LOW").alias("sqft_tier"),
    )
    .agg(
        F.avg("sqft").alias("avg_sqft"),
        F.count_if(F.col("region") == "West").alias("west_events"),
    )
)
