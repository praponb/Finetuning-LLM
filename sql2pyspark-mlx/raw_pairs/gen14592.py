from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.partitionBy("store_id").orderBy("units_whole", "id").rowsBetween(-3, Window.currentRow)

filtered = (
    df_store_sales
    .filter(F.col("sale_date") > F.to_date(F.lit("2023-03-28")))
    .select(
        "id",
        "store_id",
        "units",
        F.date_trunc("year", "sale_date").alias("sale_date_period"),
    )
)

enriched = (
    filtered
    .alias("s")
    .join(df_stores.alias("p2"), F.col("s.store_id") == F.col("p2.id"), "left")
    .filter(F.year(F.col("p2.opened_date")) == 2023)
    .select(
        "s.id",
        "s.store_id",
        "s.units",
        "s.sale_date_period",
        F.col("p2.opened_date"),
    )
)

cleaned = (
    enriched
    .filter(F.year("opened_date") >= 2023)
    .withColumn("units_whole", F.ceil("units"))
    .withColumn("sale_date_period_label", F.date_format("sale_date_period", "yyyy-MM-dd"))
)

with_dims = (
    cleaned
    .alias("src")
    .join(df_stores.alias("s"), F.col("src.store_id") == F.col("s.id"), "inner")
    .select("src.*", F.col("s.opened_date").alias("store_opened_date"))
)

result = (
    with_dims
    .select(
        "*",
        F.sum("units").over(w_hist).alias("last4_sum_units"),
        F.min("units").over(w_hist).alias("last4_min_units"),
        F.max("units").over(w_hist).alias("last4_max_units"),
    )
)
